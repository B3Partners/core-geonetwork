//=============================================================================
//===	Copyright (C) 2001-2009 Food and Agriculture Organization of the
//===	United Nations (FAO-UN), United Nations World Food Programme (WFP)
//===	and United Nations Environment Programme (UNEP)
//===
//===	This program is free software; you can redistribute it and/or modify
//===	it under the terms of the GNU General Public License as published by
//===	the Free Software Foundation; either version 2 of the License, or (at
//===	your option) any later version.
//===
//===	This program is distributed in the hope that it will be useful, but
//===	WITHOUT ANY WARRANTY; without even the implied warranty of
//===	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
//===	General Public License for more details.
//===
//===	You should have received a copy of the GNU General Public License
//===	along with this program; if not, write to the Free Software
//===	Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
//===
//===	Contact: Jeroen Ticheler - FAO - Viale delle Terme di Caracalla 2,
//===	Rome - Italy. email: geonetwork@osgeo.org
//==============================================================================
package org.fao.geonet.arcgis;

import java.io.*;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.esri.sde.sdk.client.SeError;
import com.esri.sde.sdk.client.SeException;
import com.esri.sde.sdk.client.SeQuery;
import com.esri.sde.sdk.client.SeRow;
import com.esri.sde.sdk.client.SeSqlConstruct;

import java.sql.Types;
import org.fao.geonet.kernel.harvest.harvester.arcsde.ArcSDEConnectionType;

/**
 * 
 * Adapter to retrieve ISO metadata from an ArcSDE server.
 * 
 * @author heikki doeleman
 *
 */
public class ArcSDEMetadataAdapter extends ArcSDEConnection {

    protected String schemaVersion, customQuery;
    
	public ArcSDEMetadataAdapter(ArcSDEConnectionType connectionType, String server, int instance, String database, String username, String password, String jdbcDriver, String schemaVersion, String customQuery) {
		super(connectionType, server, instance, database, username, password, jdbcDriver);
        this.schemaVersion = schemaVersion;
        this.customQuery = customQuery;
	}
	
	private static final String METADATA_TABLE = "SDE.GDB_USERMETADATA";
	private static final String METADATA_COLUMN = "SDE.GDB_USERMETADATA.XML";
	private static final String ISO_METADATA_IDENTIFIER = "MD_Metadata";
	
    /**
     * Retrieves all metadata records found in the ArcSDE database.
     */
	public List<String> retrieveMetadata() throws Exception {
		System.out.println("start retrieve metadata");
		List<String> results = new ArrayList<String>();
        switch(connectionType) {
            case jdbc:
                results = retrieveMetadataJDBC() ;
                break;
            case arcsde:
                results = retrieveMetadataArcSDE();
                break;
            default:
                // TODO what ?? throw exception ?
        }
        return results;
    }

    private List<String> retrieveMetadataJDBC() throws Exception {
        List<String> results = new ArrayList<String>();
        
        String query;
        if("9.x".equals(schemaVersion)) {
            query = "select xml from gdb_usermetadata";
        } else if("10.x".equals(schemaVersion)) {
            query = "select documentation from gdb_items";
        } else if("custom".equals(schemaVersion)) {
            query = customQuery;
        } else {
            throw new IllegalArgumentException("Invalid schemaVersion parameter: " + schemaVersion);
        }
                
        PreparedStatement statement = jdbcConnection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();
        while(resultSet.next()){
            String document = "";

            if (resultSet.getMetaData().getColumnType(1) == Types.BLOB
                    || resultSet.getMetaData().getColumnType(1) == Types.LONGVARBINARY) {
                byte[] bdata = resultSet.getBytes(1);
                document = new String(bdata);
            } else {
                document = resultSet.getString(1);
            }
            results.add(document);
        }
        System.out.println("ARCSDE Harvester using JDBC found # " + results.size() + " results");
        return results;
    }
    
    private List<String> retrieveMetadataArcSDE() throws Exception {
        List<String> results = new ArrayList<String>();            
		try {	
			// query table containing XML metadata
			SeSqlConstruct sqlConstruct = new SeSqlConstruct();
			String[] tables = {METADATA_TABLE };
			sqlConstruct.setTables(tables);
			String[] propertyNames = { METADATA_COLUMN };			
			SeQuery query = new SeQuery(seConnection);
			query.prepareQuery(propertyNames, sqlConstruct);
			query.execute();
			
			// it is not documented in the ArcSDE API how you know there are no more rows to fetch!
			// I'm assuming: query.fetch returns null (empiric tests indicate this assumption is correct).
			boolean allRowsFetched = false;
			while(! allRowsFetched) {
				SeRow row = query.fetch();
				if(row != null) {
					ByteArrayInputStream bytes = row.getBlob(0);
					byte [] buff = new byte[bytes.available()];
					bytes.read(buff);
					String document = new String(buff);
					if(document.contains(ISO_METADATA_IDENTIFIER)) {
						System.out.println("ISO metadata found");
						results.add(document);
					}
				}
				else {
					allRowsFetched = true;
				}
			}			
			query.close();
			System.out.println("ArcSDEMetadataAdapter finished retrieving metadata, found: #" + results.size() + " metadata records");
			return results;
		}
		catch(SeException x) {
			SeError error = x.getSeError();
			String description = error.getExtError() + " " + error.getExtErrMsg() + " " + error.getErrDesc();
			System.out.println(description);
			x.printStackTrace();
			throw new Exception(x);
		}
	}
}
