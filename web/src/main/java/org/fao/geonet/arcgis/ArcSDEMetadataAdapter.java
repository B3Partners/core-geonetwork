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

	public ArcSDEMetadataAdapter(ArcSDEConnectionType connectionType, String server, int instance, String database, String username, String password) {
		super(connectionType, server, instance, database, username, password);
	}
	
	private static final String METADATA_TABLE = "SDE.GDB_USERMETADATA";
	private static final String METADATA_COLUMN = "SDE.GDB_USERMETADATA.XML";
    private static final String METADATA_COLUMN_SHORT = "XML";
	private static final String ISO_METADATA_IDENTIFIER = "MD_Metadata";
    private static final String SQL_QUERY = "SELECT " + METADATA_COLUMN + " FROM " + METADATA_TABLE ;
	
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
        PreparedStatement statement = jdbcConnection.prepareStatement(SQL_QUERY);
        ResultSet resultSet = statement.executeQuery();
        while(resultSet.next()){
            String document = "";
            int colId = resultSet.findColumn(METADATA_COLUMN_SHORT);
            // very simple type check:
            if (resultSet.getMetaData().getColumnType(colId) == Types.BLOB) {
                Blob blob = resultSet.getBlob(METADATA_COLUMN_SHORT);
                byte[] bdata = blob.getBytes(1, (int) blob.length());
                document = new String(bdata);

            } else if (resultSet.getMetaData().getColumnType(colId) == Types.LONGVARBINARY) {            
                byte[] bdata = resultSet.getBytes(colId);
                document = new String(bdata);

            } else {
                throw new Exception("Trying to harvest from a column with an invalid datatype: " + resultSet.getMetaData().getColumnTypeName(colId));
                /*Reader reader = resultSet.getCharacterStream(colId);
                BufferedReader bufReader = new BufferedReader(reader);

                char[] charBuf = new char[65536];
                StringBuffer stringBuf = new StringBuffer();

                int readThisTime = bufReader.read(charBuf, 0, 65536);
                while (readThisTime != -1) {
                    stringBuf.append(charBuf, 0, readThisTime);
                    readThisTime = bufReader.read(charBuf, 0, 65536);
                }
                
                document = stringBuf.toString();*/
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
