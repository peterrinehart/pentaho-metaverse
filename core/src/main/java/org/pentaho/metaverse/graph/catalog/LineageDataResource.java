package org.pentaho.metaverse.graph.catalog;

import java.util.ArrayList;
import java.util.List;

public class LineageDataResource {

  private String name;
  private String path;
  private List<String> fields;
  private List<FieldLevelRelationship> fieldRelationships = new ArrayList<>();
  private String catalogResourceID;
  private Object vertexId;
  private String dbSchema;
  private String dbHost;
  private String dbName;

  public LineageDataResource(String name ) {
    this.name = name;
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
}
