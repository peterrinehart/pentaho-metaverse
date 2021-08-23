package org.pentaho.metaverse.graph.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.CatalogClientException;
import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import com.pentaho.di.plugins.catalog.api.entities.search.Facet;
import com.pentaho.di.plugins.catalog.api.entities.search.PagingCriteria;
import com.pentaho.di.plugins.catalog.api.entities.search.SearchCriteria;
import com.pentaho.di.plugins.catalog.api.entities.search.SearchResult;
import com.pentaho.di.plugins.catalog.api.entities.search.SortBySpecs;
import com.pentaho.di.plugins.catalog.common.CatalogClientBuilderUtil;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import com.pentaho.di.plugins.catalog.read.ReadPayload;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.pentaho.di.plugins.catalog.api.entities.search.SearchCriteria.CATALOG_DEFAULT_LIMIT;

public class CatalogLineageClient {

  private static final Logger log = LogManager.getLogger( CatalogLineageClient.class );

  private String catalogUrl;
  private String catalogUsername;
  private String catalogPassword;
  private String catalogTokenUrl;
  private String catalogClientId;
  private String catalogClientSecret;

  public CatalogLineageClient( String catalogUrl,
                                  String catalogUsername,
                                  String catalogPassword,
                                  String catalogTokenUrl,
                                  String catalogClientId,
                                  String catalogClientSecret ) {
    this.catalogUrl = catalogUrl;
    this.catalogUsername = catalogUsername;
    this.catalogPassword = catalogPassword;
    this.catalogTokenUrl = catalogTokenUrl;
    this.catalogClientId = catalogClientId;
    this.catalogClientSecret = catalogClientSecret;
  }

  public void processLineage( List<LineageDataResource> inputSources, List<LineageDataResource> outputTargets ) throws Exception {

    getExistingResourceIDs( inputSources );
    getExistingResourceIDs( outputTargets );

    if ( !hasCatalogIDs( inputSources ) || !hasCatalogIDs( outputTargets ) ) {
      log.info( "Either targets or sources IDs could not be found. Can't create lineage." );
      return;
    }

    outputTargets.forEach( dataResource -> {
      if ( dataResource.getCatalogResourceID() != null ) {
        deleteExistingLineageRelations( dataResource.getCatalogResourceID() );
      }
    } );

    outputTargets.forEach( outDataResource -> {
      if ( outDataResource.getCatalogResourceID() != null ) {
        inputSources.forEach( inputDataResource -> {
          if ( inputDataResource.getCatalogResourceID() != null ) {
            log.info( "Adding lineage relation  between " + inputDataResource.getCatalogResourceID() + " and " + outDataResource.getCatalogResourceID() + "." );
            addParentLineage( inputDataResource.getCatalogResourceID(), outDataResource.getCatalogResourceID() );
            log.info( "Lineage relation created between " + inputDataResource.getCatalogResourceID() + " and " + outDataResource.getCatalogResourceID() + "." );
          }
        } );
      }
    } );

    outputTargets.forEach( dataResource -> {
      dataResource.getFieldLevelRelationships().forEach( fieldRelationship -> {
        log.info( "Create field relationship: " + fieldRelationship );
        addFieldLevelLineage( fieldRelationship );
      } );
    } );

  }

  private void getExistingResourceIDs( List<LineageDataResource> dataResources ) {
    dataResources.forEach( dataResource -> {
      String id = searchResourceByName( dataResource.getName() );
      if ( id != null ) {
        dataResource.setCatalogResourceID( id );
      }
    } );
  }

  // TODO: this needs to be greatly improved to ensure we do the most precise search possible
  // Since transformations reading from S3/minio and other cloud storage locations don't contain the connection
  // details, we may need to search by path and file name and hope that there aren't multiple data sources
  // in the catalog with the same folder/file structures.
  // Need to search for DB tables by host/database/schema/table
  private String searchResourceByName( String resourceName ) {

    String resourceId = null;

    try {

      SearchCriteria.SearchCriteriaBuilder searchCriteriaBuilder = new SearchCriteria.SearchCriteriaBuilder();
      searchCriteriaBuilder.searchPhrase( resourceName );
      searchCriteriaBuilder.addFacet( Facet.RESOURCE_TAGS, "" );
      searchCriteriaBuilder.addFacet( Facet.VIRTUAL_FOLDERS, "" );
      searchCriteriaBuilder.addFacet( Facet.DATA_SOURCES, "" );
      searchCriteriaBuilder.addFacet( Facet.RESOURCE_TYPE, "" );
      searchCriteriaBuilder.addFacet( Facet.FILE_SIZE, "" );
      searchCriteriaBuilder.addFacet( Facet.FILE_FORMAT, "" );
      searchCriteriaBuilder.pagingCriteria( new PagingCriteria( 0, StringUtils.isNumeric( "1000" ) ? Integer.valueOf( "1000" ) : CATALOG_DEFAULT_LIMIT ) )
              .sortBySpecs( Collections.singletonList( new SortBySpecs( ReadPayload.SCORE, false ) ) )
              .entityScope( Collections.singletonList( ReadPayload.DATA_RESOURCE ) ).searchType( ReadPayload.ADVANCED )
              .preformedQuery( false ).showCollectionMembers( true ).build();

      CatalogClient catalogClient = getCatalogClient();
      SearchResult result = catalogClient.getSearch().doNew( searchCriteriaBuilder.build() );
      for ( DataResource dataResource : result.getEntities() ) {
        if ( dataResource.getResourcePath().endsWith( "/" + resourceName ) ) {
          resourceId = dataResource.getKey();
        }
      }

    } catch ( CatalogClientException e ) {
      log.error( e.getMessage(), e );
    }

    return resourceId;
  }

  private List<String> getFileLineageRelations( String resourceID ) {
    HashSet<String> lineageRelations = new HashSet<>();
    try {
      String getLineageInfo = "/api/v2/lineage/multihop/" + resourceID + "?fieldLineage=false&field=&upstream=1&downstream=0&lineageType=ACCEPTED%2CSUGGESTED";
      CatalogClient catalogClient = getCatalogClient();
      CloseableHttpResponse httpResponse = catalogClient.doGet( getLineageInfo );
      String json_string = EntityUtils.toString( httpResponse.getEntity() );
      JSONObject jsonResponse = (JSONObject) new JSONParser().parse( json_string );
      if ( jsonResponse.get( "edges" ) != null ) {
        ((JSONArray) jsonResponse.get( "edges" ) ).forEach( node -> {
          JSONObject jsonObject = (JSONObject) node;
          String type = (String) jsonObject.get( "type" );
          if ( type != null && type.equals( "edge" ) ) {
            String lineageRelationID = (String) jsonObject.get( "target" );
            lineageRelationID = lineageRelationID.substring( 0, lineageRelationID.lastIndexOf( "_" ) );
            lineageRelations.add( lineageRelationID );
          }
        } );
      }
    } catch ( IOException | CatalogClientException | ParseException e ) {
      log.error( e.getMessage(), e );
    }
    return new ArrayList<>( lineageRelations );
  }

  private List<String> getFieldLineageRelations( String resourceID, String fieldName ) {
    HashSet<String> lineageRelations = new HashSet<>();
    try {
      String getLineageInfo = "/api/v2/lineage/multihop/" + resourceID + "?fieldLineage=true&field=" + fieldName + "&upstream=1&downstream=0&lineageType=ACCEPTED,SUGGESTED";
      CatalogClient catalogClient = getCatalogClient();
      CloseableHttpResponse httpResponse = catalogClient.doGet( getLineageInfo );
      String json_string = EntityUtils.toString( httpResponse.getEntity() );
      JSONObject jsonResponse = (JSONObject) new JSONParser().parse( json_string );
      if ( jsonResponse.get( "edges" ) != null ) {
        ((JSONArray) jsonResponse.get( "edges" ) ).forEach( node -> {
          JSONObject jsonObject = (JSONObject) node;
          String type = (String) jsonObject.get( "type" );
          if ( type != null && type.equals( "edge" ) ) {
            String lineageRelationID = (String) jsonObject.get( "target" );
            lineageRelationID = lineageRelationID.substring( 0, lineageRelationID.lastIndexOf( "_" ) );
            String parentField = (String) jsonObject.get( "source" );
            parentField = parentField.substring( parentField.indexOf( "_" ) + 1, parentField.lastIndexOf( "_" ) );
            String parentID = (String) jsonObject.get( "source" );
            parentID = parentID.substring( 0, parentID.indexOf( "_" ) );
            lineageRelations.add( parentID + "|" + parentField + "|" + lineageRelationID );
          }
        } );
      }
    } catch ( IOException | CatalogClientException | ParseException e ) {
      log.error( e.getMessage(), e );
    }
    return new ArrayList<>( lineageRelations );
  }

  private void addParentLineage( String parentResourceID, String targetResourceID ) {

    try {
      List<String> existingLineageRelations = getFileLineageRelations( targetResourceID );
      String addParentURL = "/api/v2/lineage/addparent/" + targetResourceID;
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode childNode = objectMapper.createObjectNode();
      ((ObjectNode) childNode).put( "relatedKey", parentResourceID );
      if ( existingLineageRelations.size() > 0 ) {
        String lineageRelationID = existingLineageRelations.get( 0 );
        ((ObjectNode) childNode).put( "operationExecutionKey", lineageRelationID );
        addParentURL = addParentURL + "?opKey=" + lineageRelationID;
      }
      String body = objectMapper.writeValueAsString( childNode );
      StringEntity entity = new StringEntity( body );
      CatalogClient catalogClient = getCatalogClient();
      CloseableHttpResponse httpResponse = catalogClient.doPost( addParentURL, entity );
      //System.out.println( "######################################################################################" );
      //System.out.println( EntityUtils.toString( httpResponse.getEntity() ) );
      //System.out.println( "######################################################################################" );
    } catch ( IOException | CatalogClientException e ) {
      log.error( e.getMessage(), e );
    }
  }

  private void addFieldLevelLineage( FieldLevelRelationship fieldRelationship ) {

    String targetResourceID = fieldRelationship.getOutputTargetResource().getCatalogResourceID();
    String targetFieldName = fieldRelationship.getOutputTargetResourceField();
    String parentResourceID = fieldRelationship.getInputSourceResource().getCatalogResourceID();
    String parentFieldName = fieldRelationship.getInputSourceResourceField();
    List<String> fileRelationships = getFileLineageRelations( targetResourceID );
    List<String> fieldRelationships = getFieldLineageRelations( targetResourceID, targetFieldName );
    boolean relationshipExists = false;
    HashSet<String> existingFieldRelations = new HashSet<>();

    for ( String relationship : fieldRelationships ) {
      String relParentResourceID = relationship.split( "\\|" )[0];
      String relParentField = relationship.split( "\\|" )[1];
      String relID = relationship.split( "\\|" )[2];

      existingFieldRelations.add( relID );
      if ( relParentResourceID.equals( parentResourceID ) && relParentField.equals( parentFieldName ) ) {
        relationshipExists = true;
      }
    }

    if ( relationshipExists ) {
      log.info( "Relationship already exists." );
      return;
    }

    try {
      String addFieldURL = "/api/v2/lineage/addparentfield/" + targetResourceID + "_" + targetFieldName;
      if ( existingFieldRelations.size() > 0 ) {
        addFieldURL = addFieldURL + "?opKey=" + existingFieldRelations.iterator().next();
      }
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode childNode = objectMapper.createObjectNode();
      ((ObjectNode) childNode).put( "relatedKey", parentResourceID + "_" + parentFieldName );
      ((ObjectNode) childNode).put( "operationExecutionKey", fileRelationships.get( 0 ) );
      String body = objectMapper.writeValueAsString( childNode );
      StringEntity entity = new StringEntity( body );
      CatalogClient catalogClient = getCatalogClient();
      CloseableHttpResponse httpResponse = catalogClient.doPost( addFieldURL, entity );
      log.info( "Created field relationship: " + fieldRelationship );
    } catch ( IOException | CatalogClientException e ) {
      log.error( e.getMessage(), e );
    }

  }

  private void deleteExistingLineageRelations( String resourceID ) {
      getFileLineageRelations( resourceID ).forEach( this::deleteExistingLineageRelation );
  }

  private void deleteExistingLineageRelation( String lineageRelationID ) {
    try {
      String deleteLineageRelation = "/api/v2/lineage/" + lineageRelationID;
      CatalogClient catalogClient = getCatalogClient();
      catalogClient.doDelete( deleteLineageRelation );
      log.info( "Deleted old lineage relation " + lineageRelationID );
    } catch ( IOException | CatalogClientException e ) {
      log.error( e.getMessage(), e );
    }
  }

  private CatalogClient getCatalogClient() {
    CatalogDetails catalogDetails = new CatalogDetails();
    catalogDetails.setAuthType( "1" );
    catalogDetails.setUrl( catalogUrl );
    catalogDetails.setUsername( catalogUsername );
    catalogDetails.setPassword( catalogPassword );
    catalogDetails.setTokenUrl( catalogTokenUrl );
    catalogDetails.setClientId( catalogClientId );
    catalogDetails.setClientSecret( catalogClientSecret );

    CatalogClient catalogClient;
    try {
      CatalogClientBuilderUtil catalogClientBuilderUtil = new CatalogClientBuilderUtil( catalogDetails );
      catalogClient = catalogClientBuilderUtil.getCatalogClient();
      catalogClient.login();
    } catch ( CatalogClientException cce ) {
      throw new RuntimeException( cce );
    }
    return catalogClient;
  }

  private boolean hasCatalogIDs( List<LineageDataResource> dataResources ) {
    boolean catalogIDsFound = false;
    for ( LineageDataResource dataResource : dataResources ) {
      if ( !StringUtils.isEmpty( dataResource.getCatalogResourceID() ) ) {
        catalogIDsFound = true;
      }
    }
    return catalogIDsFound;
  }
}
