package org.pentaho.metaverse.graph.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.pentaho.metaverse.util.MetaverseUtil.safeListMatch;
import static org.pentaho.metaverse.util.MetaverseUtil.safeStringMatch;

public class LineageDataResource {

  private final String name;
  private String path;
  private List<String> fields;
  private final List<FieldLevelRelationship> fieldRelationships = new ArrayList<>();
  private String catalogResourceID;
  private Object vertexId;
  private String dbSchema;
  private String dbHost;
  private String dbName;
  private String dbPort;

  public LineageDataResource(String name ) {
    this.name = name;
    path = "";
    catalogResourceID = "";
    vertexId = null;
    dbSchema = "";
    dbHost = "";
    dbName = "";
    dbPort = "";
  }

  public String getName() {
    return name;
  }

  public List<String> getFields() {
    return fields;
  }

  public void setFields( List<String> fields ) {
    this.fields = fields;
  }

  public String getPath() {
    return path;
  }

  public void setPath( String path ) {
    this.path = path;
  }

  public List<FieldLevelRelationship> getFieldLevelRelationships() {
    return fieldRelationships;
  }

  public void addFieldLevelRelationship( FieldLevelRelationship fieldRelationship ) {
    fieldRelationships.add( fieldRelationship );
  }

  public void setCatalogResourceID( String catalogResourceID ) {
    this.catalogResourceID = catalogResourceID;
  }

  public String getCatalogResourceID() {
    return catalogResourceID;
  }

  public Object getVertexId() {
    return vertexId;
  }

  public void setVertexId( Object vertexId ) {
    this.vertexId = vertexId;
  }

  public String getDbSchema() {
    return dbSchema;
  }

  public void setDbSchema( String dbSchema ) {
    this.dbSchema = dbSchema;
  }

  public String getDbHost() {
    return dbHost;
  }

  public void setDbHost( String dbHost ) {
    this.dbHost = dbHost;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName( String dbName ) {
    this.dbName = dbName;
  }

  public String getDbPort() {
    return dbPort;
  }

  public void setDbPort( String dbPort ) {
    this.dbPort = dbPort;
  }

  @Override
  public String toString() {
    return "name: ".concat( name )
      .concat( " path: " ).concat( path )
      .concat( " catalogResourceId: " ).concat( catalogResourceID )
      .concat( " dbHost: " ).concat( dbHost )
      .concat( " dbPort: " ).concat( dbPort )
      .concat( " dbSchema: " ).concat( dbSchema );
  }

  @Override
  public boolean equals( Object o ) {
    if ( o instanceof LineageDataResource ) {
      LineageDataResource r2 = ( LineageDataResource ) o;
      return safeStringMatch( this.getName(), r2.getName() )
        && ( safeStringMatch( this.getPath(), r2.getPath() ) || safeStringMatch( this.getDbSchema(),
        r2.getDbSchema() ) )
        && safeListMatch( this.getFields(), r2.getFields() );
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash( name, path, fields, fieldRelationships, catalogResourceID, vertexId, dbSchema, dbName, dbHost, dbPort );
  }

}
