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
import org.pentaho.metaverse.graph.catalog.LineageDataResource;
import org.pentaho.metaverse.step.StepAnalyzerValidationIT;

import java.util.List;

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

    Mockito.verify( mockCatalogLineageClient ).processLineage( inputSourceCaptor.capture(), outputSourcesCaptor.capture() );
    Assert.assertNotNull( "input sources must not be null", inputSourceCaptor.getValue() );
    // TODO: better validation here
  }
}