package org.pentaho.metaverse.graph.catalog;

public class FieldLevelRelationship {

  private LineageDataResource inputSourceResource;
  private String inputSourceResourceField;
  private LineageDataResource outputTargetResource;
  private String outputTargetResourceField;

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
}
