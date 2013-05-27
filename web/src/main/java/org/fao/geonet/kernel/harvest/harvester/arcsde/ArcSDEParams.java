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
package org.fao.geonet.kernel.harvest.harvester.arcsde;

import jeeves.exceptions.BadInputEx;
import jeeves.utils.Util;
import org.fao.geonet.kernel.DataManager;
import org.fao.geonet.kernel.harvest.harvester.AbstractParams;
import org.jdom.Element;

/**
 * 
 * @author heikki doeleman
 *
 */
public class ArcSDEParams extends AbstractParams {

	/**
	 * Name of the ArcSDE server.
	 */
	public String server;
	/**
	 * Port number to use for connecting to the ArcSDE server ("instance").
	 */
	public int port;
	/**
	 * Username for the ArcSDE database.
	 */
	public String username;
	/**
	 * Password for the username to the ArcSDE database.
	 */
	public String password;
	/**
	 * Name of the ArcSDE database.
	 */
	public String database;
    /**
     * Whether to connect using JDBC or using ArcSDE API.
     */
    public String connectionType;
    /**
     * Driver to use for JDBC.
     */
    public String jdbcDriver;
    /**
     * For JDBC, which SDE schema (9.x or 10.x), to determine which table to
     * look at for metadata. Value "custom" to use custom query.
     */
    public String schemaVersion;
    /**
     * If the standard query for the SDE schema is not suitable use this SQL
     * query to retrieve metadata. The first column in the resultset should 
     * return the metadata in varchar, clob or blob format.
     */
    public String customQuery;

	public String icon;

	public ArcSDEParams(DataManager dm) {
		super(dm);
	}
	
	/**---------------------------------------------------------------------------
	//---
	//--- Create : called when a new entry must be added. Reads values from the
	//---          provided entry, providing default values
	//---
	//--------------------------------------------------------------------------- */
	public void create(Element node) throws BadInputEx {
		super.create(node);
		Element site = node.getChild("site");
		server = Util.getParam(site, "server", "");
		port = Util.getParam(site, "port", 0);
		username = Util.getParam(site, "username", "");
		password = Util.getParam(site, "password", "");
		database = Util.getParam(site, "database", "");
        connectionType = Util.getParam(site, "connectiontype", "jdbc");
		icon = Util.getParam(site, "icon", "arcsde.gif");
        jdbcDriver = Util.getParam(site, "jdbcDriver", "oracle.jdbc.OracleDriver");
        schemaVersion = Util.getParam(site, "schemaVersion", "9.x");
        customQuery = Util.getParam(site, "customQuery", "");
	}

	//---------------------------------------------------------------------------
	//---
	//--- Update : called when an entry has changed and variables must be updated
	//---
	//---------------------------------------------------------------------------

	public void update(Element node) throws BadInputEx {
		super.update(node);
		Element site = node.getChild("site");
		server = Util.getParam(site, "server", "");
		port = Util.getParam(site, "port", 5151);
		username = Util.getParam(site, "username", "");
		password = Util.getParam(site, "password", "");
		database = Util.getParam(site, "database", "");
        connectionType = Util.getParam(site, "connectiontype", "jdbc");
		icon = Util.getParam(site, "icon", "arcsde.gif");
        jdbcDriver = Util.getParam(site, "jdbcDriver", "oracle.jdbc.OracleDriver");
        schemaVersion = Util.getParam(site, "schemaVersion", "9.x");
        customQuery = Util.getParam(site, "customQuery", "");
	}
	
	//---------------------------------------------------------------------------
	//---
	//--- Private methods
	//---
	//---------------------------------------------------------------------------
	
	public ArcSDEParams copy() {
		ArcSDEParams copy = new ArcSDEParams(dm);
		copyTo(copy);
		copy.icon = icon;
		copy.server = server;
		copy.port = port;
		copy.username = username;
		copy.password = password;
        copy.database = database;
        copy.connectionType = connectionType;
        copy.jdbcDriver = jdbcDriver;
        copy.schemaVersion = schemaVersion;
        copy.customQuery = customQuery;
		return copy;
	}
}
