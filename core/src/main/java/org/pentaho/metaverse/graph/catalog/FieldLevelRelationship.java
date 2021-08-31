package org.pentaho.metaverse.graph.catalog;

import java.util.Objects;

import static org.pentaho.metaverse.util.MetaverseUtil.safeStringMatch;

public class FieldLevelRelationship {

  private LineageDataResource inputSourceResource;
  private String inputSourceResourceField;
  private LineageDataResource outputTargetResource;
  private String outputTargetResourceField;

  public FieldLevelRelationship() {
    inputSourceResourceField = null;
    inputSourceResource = null;
    outputTargetResourceField = null;
    outputTargetResource = null;
  }

  public FieldLevelRelationship( LineageDataResource inputSourceResource, LineageDataResource outputTargetResource,
                                 String inputSourceResourceField, String outputTargetResourceField ) {
    this.inputSourceResource = inputSourceResource;
    this.outputTargetResource = outputTargetResource;
    this.inputSourceResourceField = inputSourceResourceField;
    this.outputTargetResourceField = outputTargetResourceField;
  }

  public LineageDataResource getInputSourceResource() {
    return inputSourceResource;
  }

  public void setInputSourceResource( LineageDataResource inputSourceResource ) {
    this.inputSourceResource = inputSourceResource;
  }

  public String getInputSourceResourceField() {
    return inputSourceResourceField;
  }

  public void setInputSourceResourceField( String inputSourceResourceField ) {
    this.inputSourceResourceField = inputSourceResourceField;
  }

  public LineageDataResource getOutputTargetResource() {
    return outputTargetResource;
  }

  public void setOutputTargetResource( LineageDataResource outputTargetResource ) {
    this.outputTargetResource = outputTargetResource;
  }

  public String getOutputTargetResourceField() {
    return outputTargetResourceField;
  }

  public void setOutputTargetResourceField( String outputTargetResourceField ) {
    this.outputTargetResourceField = outputTargetResourceField;
  }

  @Override
  public String toString() {
    return inputSourceResource.getName() + ":" + inputSourceResourceField
            + " -> "
            + outputTargetResource.getName() + ":" + outputTargetResourceField;
  }

  @Override
  public boolean equals( Object o ) {
    if ( o instanceof FieldLevelRelationship ) {
      FieldLevelRelationship r2 = ( FieldLevelRelationship ) o;
      return ( ( this.inputSourceResource != null && r2.inputSourceResource != null && this.inputSourceResource.equals( r2.inputSourceResource ) )
        || ( this.inputSourceResource == null && r2.inputSourceResource == null ) )
        && ( ( this.outputTargetResource != null && r2.outputTargetResource != null && this.outputTargetResource.equals( r2.outputTargetResource ) )
        || ( this.outputTargetResource == null && r2.outputTargetResource == null ) )
        && safeStringMatch( this.inputSourceResourceField, r2.inputSourceResourceField )
        && safeStringMatch( this.outputTargetResourceField, r2.outputTargetResourceField );
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash( inputSourceResource, inputSourceResourceField, outputTargetResource, outputTargetResourceField );
  }
}
