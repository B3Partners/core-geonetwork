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
import java.sql.SQLException;

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
	public List<Object[]> retrieveMetadata() throws Exception {
		System.out.println("start retrieve metadata");
		List<Object[]> results = new ArrayList<Object[]>();
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

    private List<Object[]> retrieveMetadataJDBC() throws Exception {
        List<Object[]> results = new ArrayList<Object[]>();
        
        String query;
        if("9.x".equals(schemaVersion)) {
            query = "select xml as metadata, id, name from gdb_usermetadata";
        } else if("10.x".equals(schemaVersion)) {
            query = "select documentation as metadata, uuid, path, physicalname from gdb_items";
        } else if("custom".equals(schemaVersion)) {
            query = customQuery;
        } else {
            throw new IllegalArgumentException("Invalid schemaVersion parameter: " + schemaVersion);
        }
                
        PreparedStatement statement = jdbcConnection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery();
        while(resultSet.next()){
            byte[] document = null;

            int metadataIndex;
            try {
                metadataIndex = resultSet.findColumn("metadata");
            } catch(SQLException e) {
                // No column named metadata, use first column
                metadataIndex = 1;
            }
            if (resultSet.getMetaData().getColumnType(metadataIndex) == Types.BLOB
                    || resultSet.getMetaData().getColumnType(metadataIndex) == Types.LONGVARBINARY) {
                document = resultSet.getBytes(metadataIndex);
            } else {
                document = resultSet.getString(metadataIndex).getBytes("UTF-8");
            }
            String otherInfo = "length " + (document == null ? 0 : document.length);
            if(document != null) {
                int sampleLength = 60;
                byte[] sampleBytes = new byte[Math.min(sampleLength, document.length)];
                System.arraycopy(document, 0, sampleBytes, 0, sampleBytes.length);
                otherInfo = otherInfo + ", sample: \"" + new String(sampleBytes, "US-ASCII") 
                        + (document.length > sampleLength ? "..." : "") + "\"";
            }
            for(int i = 1; i < resultSet.getMetaData().getColumnCount()+1; i++) {
                if(i != metadataIndex) {
                    try {
                        String s = otherInfo.equals("") ? "" : ", ";
                        String n = resultSet.getMetaData().getColumnName(i);
                        if(n == null || "".equals(n)) {
                            n = "column " + i;
                        }
                        s += n + "=" + resultSet.getString(i);
                        otherInfo += s;
                    } catch(SQLException e) {
                    }
                }
            }
            
            results.add(new Object[] {document, otherInfo});
        }
        System.out.println("ARCSDE Harvester using JDBC found # " + results.size() + " results");
        return results;
    }
    
    private List<Object[]> retrieveMetadataArcSDE() throws Exception {
        List<Object[]> results = new ArrayList<Object[]>();
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
						results.add(new Object[]{buff,""});
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
