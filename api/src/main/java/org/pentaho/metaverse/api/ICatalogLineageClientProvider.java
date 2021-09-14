package org.pentaho.metaverse.api;

public interface ICatalogLineageClientProvider {

  ICatalogLineageClient getCatalogLineageClient( String catalogUrl,
                                                 String catalogUsername,
                                                 String catalogPassword,
                                                 String catalogTokenUrl,
                                                 String catalogClientId,
                                                 String catalogClientSecret );
}
