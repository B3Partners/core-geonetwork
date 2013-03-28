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

import com.esri.sde.sdk.client.SeConnection;
import com.esri.sde.sdk.client.SeError;
import com.esri.sde.sdk.client.SeException;
import org.fao.geonet.kernel.harvest.harvester.arcsde.ArcSDEConnectionType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 
 * Adapter for ArcSDE connections. Can use either the ArcSDE Java API, or a direct JDBC connection to
 * ArcSDE's database. Currently for JDBC only Oracle is supported (driver name is hardcoded).
 * 
 * See http://edndoc.esri.com/arcsde/9.3/api/japi/japi.htm for (very little) information
 * about the ArcSDE Java API.
 * 
 * @author heikki doeleman
 *
 */
public class ArcSDEConnection {

    protected ArcSDEConnectionType connectionType;
	protected SeConnection seConnection ;
    protected Connection jdbcConnection ;
	
	/**
	 * Opens a connection to the specified ArcSDE server.  Connectiontype can be either arcsde or jdbc.
     *
     * An example of server string in case of jdbc is:
     * "jdbc:oracle:thin:@84.123.79.19:1521:orcl".
     *
	 * @param connectionType
	 * @param server
	 * @param instance
	 * @param database
	 * @param username
	 * @param password
	 */
	public ArcSDEConnection(ArcSDEConnectionType connectionType, String server, int instance, String database, String username, String password) {
        this.connectionType = connectionType;
        switch(connectionType) {
            case jdbc:
                try {
                    // TODO make JDBC driver configurable
                    // Load the JDBC driver
                    String driverName = "oracle.jdbc.driver.OracleDriver";
                    Class.forName(driverName).newInstance();
                    // Create a connection to the database
                    String connData = "jdbc:oracle:thin:@" + server + ":" + instance + ":" + database;

                    jdbcConnection = DriverManager.getConnection(connData, username, password);
                    System.out.println("Connected to ArcSDE using JDBC");
                }
                catch (ClassNotFoundException x) {
                    System.out.println(x.getMessage());
                    x.printStackTrace();
                    throw new ExceptionInInitializerError(new ArcSDEConnectionException("Exception in ArcSDEConnection using JDBC: can not find the database driver", x));
                }
                catch (SQLException x) {
                    System.out.println(x.getMessage());
                    x.printStackTrace();
                    throw new ExceptionInInitializerError(new ArcSDEConnectionException("Exception in ArcSDEConnection using JDBC: can not connect to the database", x));
                }
                catch (InstantiationException x) {
                    System.out.println(x.getMessage());
                    x.printStackTrace();
                    throw new ExceptionInInitializerError(new ArcSDEConnectionException("Exception in ArcSDEConnection using JDBC: can not instantiate the database driver", x));
                }
                catch (IllegalAccessException x) {
                    System.out.println(x.getMessage());
                    x.printStackTrace();
                    throw new ExceptionInInitializerError(new ArcSDEConnectionException("Exception in ArcSDEConnection using JDBC: can not access the database driver", x));                    
                }
                break;
            case arcsde:
                try {
    			    seConnection = new SeConnection(server, instance, database, username, password);
    	    		System.out.println("Connected to ArcSDE using ARCSDE API");
	    	    	seConnection.setConcurrency(SeConnection.SE_LOCK_POLICY);
		        }
		        catch (SeException x) {
        			SeError error = x.getSeError();
		        	String description = error.getExtError() + " " + error.getExtErrMsg() + " " + error.getErrDesc();
        			System.out.println(description);
		        	x.printStackTrace();
			        throw new ExceptionInInitializerError(new ArcSDEConnectionException("Exception in ArcSDEConnection using ARCSDE: " + description, x));
        		}
                break;
            default:
                throw new ExceptionInInitializerError(new ArcSDEConnectionException("Unknown connection type: " + connectionType.name()));
        }
    }
	
	/**
	 * Closes the connection to the ArcSDE server.
	 */
	public void close() throws ArcSDEConnectionException {
        switch(connectionType) {
            case jdbc:
                try {
                    jdbcConnection.close();
                }
                catch (SQLException x) {
                    x.printStackTrace();
                    throw new ArcSDEConnectionException("Exception closing JDBC connection", x);
                }
                break;
            case arcsde:
                try {
                    seConnection.close();
                }
                catch (SeException x) {
                    x.printStackTrace();
                    throw new ArcSDEConnectionException("Exception closing ARCSDE connection", x);
                }
                break;
            default:
                throw new ArcSDEConnectionException("Unknown connection type: " + connectionType.name());
        }
	}
	
	/**
	 * Closes the connection to ArcSDE server in case users of this class neglect to do so.
	 */
	public void finalize() throws Throwable {
        try {
            switch(connectionType) {
                case jdbc:
                    if(jdbcConnection != null) {
                        jdbcConnection.close();
                    }
                    break;
                case arcsde:
                    if(seConnection != null) {
                        seConnection.close();
                    }
                    break;
                default:
                    throw new ArcSDEConnectionException("Unknown connection type: " + connectionType.name());
            }
		}
		catch(Throwable x) {
			x.printStackTrace();
			throw new ArcSDEConnectionException("Exception finalizing class ArcSDEConnection", x);
		}
		finally {
			super.finalize();
		}
	}
}
