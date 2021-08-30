/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2021 by Hitachi Vantara : http://www.pentaho.com
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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.pentaho.metaverse.frames.TransformationNode;
import org.pentaho.metaverse.graph.catalog.CatalogLineageClient;
import org.pentaho.metaverse.graph.catalog.FieldLevelRelationship;
import org.pentaho.metaverse.graph.catalog.LineageDataResource;
import org.pentaho.metaverse.step.StepAnalyzerValidationIT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GraphCatalogWriterIT extends StepAnalyzerValidationIT {
  @Mock CatalogLineageClient mockCatalogLineageClient;
  @Captor ArgumentCaptor<List<LineageDataResource>> inputSourceCaptor;
  @Captor ArgumentCaptor<List<LineageDataResource>> outputSourcesCaptor;

  @Test
  public void testMultiSource() throws Exception {
    final String transNodeName = "CombineMultiSourceToTarget";
    initTest( transNodeName );

    //final TransformationNode transformationNode = verifyTransformationNode( transNodeName, false );
    GraphCatalogWriter graphCatalogWriter = new GraphCatalogWriter( mockCatalogLineageClient );

    graphCatalogWriter.outputGraphImpl( graph, null );

    LineageDataResource personCsv = new LineageDataResource( "person.csv" );
    personCsv.setPath( "/Users/aramos/Documents/Hitachi/REPOS/R2D2-DEV/CatalogTestKTR/person.csv" );
    List<String> personFields = Arrays.asList( "first_name", "id", "lasy_name" );
    personCsv.setFields( personFields );
    LineageDataResource personDetailsCsv = new LineageDataResource( "person_details.csv" );
    personDetailsCsv.setPath( "/Users/aramos/Documents/Hitachi/REPOS/R2D2-DEV/CatalogTestKTR/person_details.csv" );
    List<String> personDetailsFields = Arrays.asList( "gender", "ip_address", "id", "age", "email" );
    personDetailsCsv.setFields( personDetailsFields );
    LineageDataResource outputTarget = new LineageDataResource( "CombinedCsvToTextOut.csv" );
    outputTarget.setPath( "/Users/aramos/Documents/Hitachi/REPOS/R2D2-DEV/CatalogTestKTR/out/CombinedCsvToTextOut.csv" );
    List<String> outputFields = Arrays.asList( "GIVEN", "HOST", "SUR", "SPAN", "USERNAME", "LONG_LEGAL", "SSN", "SEX" );
    outputTarget.setFields( outputFields );
    addRelationship( personCsv, outputTarget, "first_name", "GIVEN" );
    addRelationship( personCsv, outputTarget, "last_name", "SUR" );
    addRelationship( personCsv, outputTarget, "id", "SSN" );
    addRelationship( personDetailsCsv, outputTarget, "ip_address", "HOST" );
    addRelationship( personDetailsCsv, outputTarget, "age", "SPAN" );
    addRelationship( personDetailsCsv, outputTarget, "email", "USERNAME" );
    addRelationship( personDetailsCsv, outputTarget, "gender", "SEX" );
    
    Mockito.verify( mockCatalogLineageClient ).processLineage( inputSourceCaptor.capture(), outputSourcesCaptor.capture() );
    List<LineageDataResource> inputSources = inputSourceCaptor.getValue();
    Assert.assertNotNull( "input sources must not be null", inputSources );
  }

  private void addRelationship( LineageDataResource input, LineageDataResource output, String inputField, String outputField ) {
    FieldLevelRelationship r1 = new FieldLevelRelationship();
    r1.setInputSourceResource( input );
    r1.setOutputTargetResource( output );
    r1.setInputSourceResourceField( inputField );
    r1.setOutputTargetResourceField( outputField );
    input.addFieldLevelRelationship( r1 );
    output.addFieldLevelRelationship( r1 );
  }

  private Optional<LineageDataResource> findInList( List<LineageDataResource> list, LineageDataResource resource ) {
    boolean found = false;
    List<LineageDataResource> possibleMatches = list.stream().filter( r ->
      // name and path are not null and match
      ( resource.getName() != null && r.getName() != null && r.getName().equals( resource.getName() )
      && ( ( resource.getPath() != null && r.getPath() != null && r.getPath().equals( resource.getPath() ) )
      || // table name and schema are not null and match
        ( resource.getDbSchema() != null && r.getDbName() != null && r.getDbSchema().equals( resource.getDbSchema() ) )
      ) ) )
      .collect( Collectors.toList() );

    return possibleMatches.stream().filter( r -> r.getFields().containsAll( resource.getFields() )
         && resource.getFields().containsAll( r.getFields() ) ).findFirst();
  }


}