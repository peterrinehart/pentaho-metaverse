/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.metaverse.graph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.Tokens;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.graph.catalog.CatalogLineageClient;
import org.pentaho.metaverse.graph.catalog.LineageDataResource;
import org.pentaho.metaverse.graph.catalog.FieldLevelRelationship;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The GraphMLWriter class contains methods for writing a metaverse graph model in GraphML format
 * 
 */
public class GraphCatalogWriter extends BaseGraphWriter {

  private static final Logger log = LogManager.getLogger( GraphCatalogWriter.class );
  private static final String PATH_NODE_SEPARATOR = " <- ";
  private static final String VERTEX_NODE = "V: ";
  private static final String EDGE_NODE = "E: ";

  private CatalogLineageClient lineageClient;

  public GraphCatalogWriter( String catalogUrl,
                             String catalogUsername,
                             String catalogPassword,
                             String catalogTokenUrl,
                             String catalogClientId,
                             String catalogClientSecret ) {
    super();
    lineageClient = new CatalogLineageClient( catalogUrl,
            catalogUsername,
            catalogPassword,
            catalogTokenUrl,
            catalogClientId,
            catalogClientSecret );
  }

  @Override
  public void outputGraphImpl( Graph graph, OutputStream out ) throws IOException {

    log.info( "Stating lineage processing." );

    ArrayList<LineageDataResource> inputSources = new ArrayList<>();
    ArrayList<LineageDataResource> outputTargets = new ArrayList<>();

    // Get input data sources and fields
    GremlinPipeline<Graph, Vertex> inputNodesPipe =
            new GremlinPipeline<Graph, Vertex>( graph )
                    .V()
                    .has( DictionaryConst.PROPERTY_TYPE, DictionaryConst.NODE_TYPE_TRANS_STEP )
                    .in( DictionaryConst.LINK_READBY )
                    .cast( Vertex.class );
    List<Vertex> inputVertexes = inputNodesPipe.toList();
    inputVertexes.forEach( vertex -> {
      String sourceName = vertex.getProperty( DictionaryConst.PROPERTY_PATH );
      if ( sourceName != null && !sourceName.equals( "" ) ) {
        LineageDataResource dataResource = new LineageDataResource( getSourceName( sourceName ) );
        dataResource.setPath( sourceName );
        dataResource.setFields( getDatasourceFields( sourceName, graph ) );
        inputSources.add( dataResource );
      }
    } );

    // Get output data sources and fields
    GremlinPipeline<Graph, Vertex> outputNodesPipe =
            new GremlinPipeline<Graph, Vertex>( graph )
                    .V()
                    .has( DictionaryConst.PROPERTY_TYPE, DictionaryConst.NODE_TYPE_TRANS_STEP )
                    .out( DictionaryConst.LINK_WRITESTO )
                    .cast( Vertex.class );
    List<Vertex> outputVertexes = outputNodesPipe.toList();
    outputVertexes.forEach( vertex -> {
      String sourceName = vertex.getProperty( DictionaryConst.PROPERTY_PATH );
      if ( sourceName != null && !sourceName.equals( "" ) ) {
        LineageDataResource dataResource = new LineageDataResource( getSourceName( sourceName ) );
        dataResource.setPath( sourceName );
        dataResource.setFields( getDatasourceFields( sourceName, graph ) );
        outputTargets.add( dataResource );
      }
    } );

    // Trace output fields to source fields
    linkTargetFieldsToSources( outputTargets, inputSources, graph );

    try {
      lineageClient.processLineage( inputSources, outputTargets );
    } catch ( Exception e ) {
      log.error( e.getMessage(), e );
    }

    log.info( "Lineage processing done." );
  }

  private String getSourceName( String fullName ) {
    String sourceName = null;
    if ( fullName.contains( "/" ) ) {
      sourceName = fullName.substring( fullName.lastIndexOf( "/" ) + 1 );
    } else {
      sourceName = fullName;
    }
    return sourceName;
  }

  private List<String> getDatasourceFields( String sourceName, Graph graph ) {
    GremlinPipeline<Graph, Vertex> inputFieldsPipe =
            new GremlinPipeline<Graph, Vertex>( graph )
                    .V()
                    .has( DictionaryConst.PROPERTY_PATH, Tokens.T.eq, sourceName )
                    .out( DictionaryConst.LINK_CONTAINS )
                    .cast( Vertex.class );
    List<Vertex> inputFieldVertexes = inputFieldsPipe.toList();
    ArrayList<String> fields = new ArrayList<>();
    inputFieldVertexes.forEach( fieldVertex -> {
      fields.add( fieldVertex.getProperty( DictionaryConst.PROPERTY_NAME ) );
    } );
    return fields;
  }

  private void linkTargetFieldsToSources( List<LineageDataResource> outputTargets, List<LineageDataResource> inputSources, Graph graph ) {
    for ( LineageDataResource outputTarget : outputTargets ) {
      GremlinPipeline<Graph, Vertex> allNodesPipe =
              new GremlinPipeline<Graph, Vertex>( graph )
                      .V()
                      .has( DictionaryConst.PROPERTY_PATH, Tokens.T.eq, outputTarget.getPath() )
                      .out( DictionaryConst.LINK_CONTAINS )
                      //.has( DictionaryConst.PROPERTY_NAME, Tokens.T.eq, "SUR" )
                      .cast( Vertex.class );
      List<Vertex> allVertexes = allNodesPipe.toList();
      allVertexes.forEach( vertex -> {
        String outputTargetResourceField = vertex.getProperty( DictionaryConst.PROPERTY_NAME );
        List<String> paths = traverseVertex( vertex );
        paths.forEach( path -> {
          inputSources.forEach( inputSource -> {
            if ( path.endsWith( inputSource.getPath() ) ) {

              String[] pathElements = path.split( PATH_NODE_SEPARATOR );
              String inputSourceField = null;
              for ( int e = pathElements.length - 2; e >= 0; e-- ) {
                if ( pathElements[e].startsWith( VERTEX_NODE ) ) {
                  inputSourceField = pathElements[e].substring( VERTEX_NODE.length() );
                  break;
                }
              }
              log.info( "Field path found: " + path );
              FieldLevelRelationship fieldRelationship = new FieldLevelRelationship();
              fieldRelationship.setInputSourceResource( inputSource );
              fieldRelationship.setInputSourceResourceField( inputSourceField );
              fieldRelationship.setOutputTargetResource( outputTarget );
              fieldRelationship.setOutputTargetResourceField( outputTargetResourceField );
              inputSource.addFieldLevelRelationship( fieldRelationship );
              outputTarget.addFieldLevelRelationship( fieldRelationship );
            }
          } );
        } );
      } );
    }
  }

  private List<String> traverseVertex( Vertex vertex ) {
    ArrayList<String> paths = new ArrayList<>();

    Iterator<Edge> edges = vertex.getEdges( Direction.IN ).iterator();
    while ( edges.hasNext() ) {
      Edge edge = edges.next();
      if ( edge.getLabel().equals( DictionaryConst.LINK_POPULATES )
        || edge.getLabel().equals( DictionaryConst.LINK_DERIVES )
        || edge.getLabel().equals( DictionaryConst.LINK_CONTAINS ) ) {
        Vertex nextVertex = edge.getVertex( Direction.OUT );
        List<String> newPaths = traverseVertex( nextVertex );
        newPaths.forEach( path -> {
          paths.add( VERTEX_NODE + vertex.getProperty( DictionaryConst.PROPERTY_NAME )
                  + PATH_NODE_SEPARATOR + EDGE_NODE + edge.getLabel()
                  + PATH_NODE_SEPARATOR + path );
        } );
      }
    }
    if ( paths.isEmpty() ) {
      paths.add( VERTEX_NODE + vertex.getProperty( DictionaryConst.PROPERTY_NAME ) );
    }
    return paths;
  }
}
