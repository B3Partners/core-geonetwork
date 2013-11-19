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

import java.io.ByteArrayInputStream;
import jeeves.exceptions.BadInputEx;
import jeeves.interfaces.Logger;
import jeeves.resources.dbms.Dbms;
import jeeves.server.context.ServiceContext;
import jeeves.server.resources.ResourceManager;
import jeeves.utils.Xml;
import org.fao.geonet.arcgis.ArcSDEMetadataAdapter;
import org.fao.geonet.constants.Geonet;
import org.fao.geonet.kernel.harvest.harvester.AbstractHarvester;
import org.fao.geonet.kernel.harvest.harvester.AbstractParams;
import org.fao.geonet.kernel.harvest.harvester.CategoryMapper;
import org.fao.geonet.kernel.harvest.harvester.GroupMapper;
import org.fao.geonet.kernel.harvest.harvester.Privileges;
import org.fao.geonet.lib.Lib;
import org.fao.geonet.resources.Resources;
import org.fao.geonet.util.ISODate;
import org.jdom.Element;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.fao.geonet.exceptions.NoSchemaMatchesException;
import org.jdom.Namespace;

/**
 *
 * Harvester from ArcSDE.
 *
 * Can be used in ARCSDE mode, in which case the ArcSDE Java API is used. When you use that mode you can use either the
 * classic ArcSDE mode or the "Direct Connect" mode. ARCSDE mod requires the propietary ESRI libraries containing their
 * API. Those are not committed to our svn, so you'll need to replace the dummy library arcsde-dummy.jar with the real
 * ones for this to work. For Direct Connect you also need some ESRI DLLs and an Oracle client on your machine. See
 * http://webhelp.esri.com/arcgisdesktop/9.2/index.cfm?TopicName=Setting_up_clients_for_a_direct_connection for
 * information on that.
 *
 * Can also be used in JDBC mode, in which case simply a direct JDBC connection to your ArcSDE database is opened. For
 * this you must include your JDBC driver for the database used by ArcSDE (for example the Oracle driver).
 *
 * @author heikki doeleman
 * @author Matthijs Laan
 *
 */
public class ArcSDEHarvester extends AbstractHarvester {

	private ArcSDEParams params;
	private ArcSDEResult result;
	
	private static final String ARC_TO_ISO19115_TRANSFORMER = "ArcCatalog8_to_ISO19115.xsl";
    private static final String ISO19115_TO_ISO19139_TRANSFORMER = "ISO19115-to-ISO19139.xsl";
	private static String ARC_TO_ISO19115_TRANSFORMER_LOCATION;
    private static String ISO19115_TO_ISO19139_TRANSFORMER_LOCATION;

	public static void init(ServiceContext context) throws Exception {
		ARC_TO_ISO19115_TRANSFORMER_LOCATION = context.getAppPath() + Geonet.Path.STYLESHEETS + "/conversion/import/" + ARC_TO_ISO19115_TRANSFORMER;
		ISO19115_TO_ISO19139_TRANSFORMER_LOCATION = context.getAppPath() + Geonet.Path.STYLESHEETS + "/conversion/import/" + ISO19115_TO_ISO19139_TRANSFORMER;
	}

    /**
     * Saves an ArcSDE harvester's properties.
     */
	@Override
	protected void storeNodeExtra(Dbms dbms, AbstractParams params, String path, String siteId, String optionsId) throws SQLException {
		ArcSDEParams as = (ArcSDEParams) params;
		settingMan.add(dbms, "id:"+siteId, "icon", as.icon);
		settingMan.add(dbms, "id:"+siteId, "server", as.server);
		settingMan.add(dbms, "id:"+siteId, "port", as.port);
		settingMan.add(dbms, "id:"+siteId, "username", as.username);
		settingMan.add(dbms, "id:"+siteId, "password", as.password);
        settingMan.add(dbms, "id:"+siteId, "database", as.database);
        settingMan.add(dbms, "id:"+siteId, "connectionType", as.connectionType);
        settingMan.add(dbms, "id:"+siteId, "jdbcDriver", as.jdbcDriver);
        settingMan.add(dbms, "id:"+siteId, "schemaVersion", as.schemaVersion);
        settingMan.add(dbms, "id:"+siteId, "customQuery", as.customQuery);
	}

    /**
     * Saves a new ArcSDE harvester.
     */
	@Override
	protected String doAdd(Dbms dbms, Element node) throws BadInputEx, SQLException {
	/*	try {
			@SuppressWarnings("unused")
			int test = GeoToolsDummyAPI.DUMMY_API_VERSION;
			// if you get here, you're using the dummy API
			System.out.println("ERROR: NO ARCSDE LIBRARIES INSTALLED");
			System.out.println("Replace arcsde-dummy.jar with the real ArcSDE libraries from ESRI");
			System.err.println("ERROR: NO ARCSDE LIBRARIES INSTALLED");
			System.err.println("Replace arcsde-dummy.jar with the real ArcSDE libraries from ESRI");
			return null;
		}
		catch(NoClassDefFoundError n) {
	*/		// using the real ESRI ArcSDE libraries : continue		
			params = new ArcSDEParams(dataMan);
		
			//--- retrieve/initialize information
			params.create(node);
		
			//--- force the creation of a new uuid
			params.uuid = UUID.randomUUID().toString();
		
			String id = settingMan.add(dbms, "harvesting", "node", getType());
			storeNode(dbms, params, "id:"+id);
			
			Lib.sources.update(dbms, params.uuid, params.name, true);
			Resources.copyLogo(context, "images" + File.separator + "harvesting" + File.separator + params.icon, params.uuid);
			
			return id;
	//	}
	}

    /**
     * Saves information about the results of the last harvesting run.
     */
	@Override
	protected void doAddInfo(Element node) {
		//--- if the harvesting is not started yet, we don't have any info

		if (result == null)
			return;

		//--- ok, add proper info

		Element info = node.getChild("info");
		Element res = getResult();
		info.addContent(res);
	}

	@Override
	protected Element getResult() {
		Element res  = new Element("result");

		if (result != null) {
			add(res, "total",          result.total);
			add(res, "added",          result.added);
			add(res, "updated",        result.updated);
			add(res, "unknownSchema",  result.unknownSchema);
			add(res, "removed",        result.removed);
			add(res, "badFormat",      result.badFormat);
			add(res, "doesNotValidate",result.doesNotValidate);
		}

		return res;
	}

    /**
     * Removes an ArcSDE harvester.
     */
	@Override
	protected void doDestroy(Dbms dbms) throws SQLException {
		File icon = new File(Resources.locateLogosDir(context), params.uuid +".gif");
		icon.delete();
		Lib.sources.delete(dbms, params.uuid);
	}

    /**
     * Executes a harvesting run.
     */
    @Override
	protected void doHarvest(Logger logger, ResourceManager rm) throws ArcSDEHarvesterException {
        try {
            ArcSDEConnectionType connectionType = ArcSDEConnectionType.valueOf(params.connectionType);
            logger.info("ArcSDE harvest starting using connection type: " + connectionType.name());
            if(connectionType == ArcSDEConnectionType.jdbc) {
                logger.info(String.format("ArcSDE harvest JDBC driver=%s, schema=%s, customQuery=%s", params.jdbcDriver, params.schemaVersion, params.customQuery));
            }
            ArcSDEMetadataAdapter adapter = new ArcSDEMetadataAdapter(connectionType, params.server, params.port, params.database, params.username, params.password, params.jdbcDriver, params.schemaVersion, params.customQuery);
            List<Object[]> metadataList = adapter.retrieveMetadata();
            align(metadataList, rm);
            logger.info("ArcSDE harvest finished");
        }
        catch(Throwable x) {
            x.printStackTrace();
           String message = x.getMessage();
           // If message is null, check getCause() (useful for ExceptionInInitializerError launched in ArcSDEConnection)
           if ((message == null) && (x.getCause() != null)) message = x.getCause().getMessage();
           logger.error(message);

           throw new ArcSDEHarvesterException("ArcSDEHarvester raised exception in doHarvest: " + message, x);
        }
	}

	private void align(List<Object[]> metadataList, ResourceManager rm) throws Exception {
		System.out.println("Start of alignment for : "+ params.name);
		this.result = new ArcSDEResult();
		Dbms dbms = (Dbms) rm.open(Geonet.Res.MAIN_DB);
        
		boolean transformIt = false;
		String thisXslt = context.getAppPath() + Geonet.Path.IMPORT_STYLESHEETS + "/";
		if (!params.importXslt.equals("none")) {
			thisXslt = thisXslt + params.importXslt;
			transformIt = true;
		}
        
		//----------------------------------------------------------------
		//--- retrieve all local categories and groups
		//--- retrieve harvested uuids for given harvesting node
		CategoryMapper localCateg = new CategoryMapper(dbms);
		GroupMapper localGroups = new GroupMapper(dbms);
		dbms.commit();		
		List<String> idsForHarvestingResult = new ArrayList<String>();
		//-----------------------------------------------------------------------
		//--- insert/update metadata		
        int count = 0;
		for(Object[] metadataInfo : metadataList) {
            byte[] metadata = (byte[])metadataInfo[0];
            String otherInfo = (String)metadataInfo[1];
            System.out.println(String.format("Processing record %d of %d, harvest info: %s",
                    ++count, metadataList.size(), otherInfo != null && !"".equals(otherInfo) ? otherInfo : "-"));
            
            if(metadata == null || metadata.length == 0) {
                result.badFormat++;
                continue;
            }
			result.total++;
			// create JDOM element from String-XML
			Element metadataElement = Xml.loadStream(new ByteArrayInputStream(metadata));
            
            // If it exists, unwrap a MD_Metadata element if contained within a
            // <metadata> ESRI document 
            
            Element iso19139;
            if("MD_Metadata".equals(metadataElement.getName()) && "http://www.isotc211.org/2005/gmd".equals(metadataElement.getNamespaceURI())) {
                iso19139 = metadataElement;
            } else {
                iso19139 = metadataElement.getChild("MD_Metadata", Namespace.getNamespace("http://www.isotc211.org/2005/gmd"));
                
                if(iso19139 == null) {
                    // transform ESRI output to ISO19115
                    Element iso19115 = Xml.transform(metadataElement, ARC_TO_ISO19115_TRANSFORMER_LOCATION);
                    // transform ISO19115 to ISO19139
                    iso19139 = Xml.transform(iso19115, ISO19115_TO_ISO19139_TRANSFORMER_LOCATION);
                }
            }
            
            String schema = null;
            try {
                schema = dataMan.autodetectSchema(iso19139);
            } catch(NoSchemaMatchesException nsm) {
            }
			if(schema == null) {
				result.unknownSchema++;
			}
			// the xml is recognizable iso19139 format
			else {
				String uuid = dataMan.extractUUID(schema, iso19139);
				if(uuid == null || uuid.equals("")) {
					System.out.println("Skipping metadata due to failure extracting uuid (uuid null or empty).");
					result.badFormat++;
				}
				else {

                    // validate it here if requested
                    if (params.validate) {
                        if(!dataMan.validate(iso19139))  {
                            System.out.println("Ignoring invalid metadata with uuid " + uuid);
                            result.doesNotValidate++;
                            continue;
                        }
                    }
                    
                    // transform using importxslt if not none
                    if (transformIt) {
                        try {
                            iso19139 = Xml.transform(iso19139, thisXslt);
                        } catch (Exception e) {
                            System.out.println("Cannot transform XML from SDE, ignoring. Error was: "+e.getMessage());
                            result.badFormat++;
                            continue; // skip this one
                        }
                    }                    

					//
					// add / update the metadata from this harvesting result
					//
					String id = dataMan.getMetadataId(dbms, uuid);
					if (id == null)	{
						System.out.println("adding new metadata");
						id = addMetadata(iso19139, uuid, dbms, schema, localGroups, localCateg);
						result.added++;
					}
					else {
						System.out.println("updating existing metadata, id is: " + id);
						updateMetadata(iso19139, id, dbms, localGroups, localCateg);
						result.updated++;
					}
					idsForHarvestingResult.add(id);
				}
			}
		}
		//
		// delete locally existing metadata from the same source if they were
		// not in this harvesting result
		//	
		List<Element> existingMetadata = dataMan.getMetadataByHarvestingSource(dbms, params.uuid);
		for(Element existingId : existingMetadata) {
			String ex$ = existingId.getChildText("id");
			if(!idsForHarvestingResult.contains(ex$)) {
                System.out.println("Deleting old record from index not in harvesting result: " + ex$);
				dataMan.deleteMetadataGroup(context, dbms, ex$);
				result.removed++;
			}
		}			
	}

	private void updateMetadata(Element xml, String id, Dbms dbms, GroupMapper localGroups, CategoryMapper localCateg) throws Exception {
		System.out.println("  - Updating metadata with id: "+ id);
        //
        // update metadata
        //
        boolean validate = false;
        boolean ufo = false;
        boolean index = false;
        String language = context.getLanguage();
        dataMan.updateMetadata(context, dbms, id, xml, validate, ufo, index, language, new ISODate().toString(), false);

		dbms.execute("DELETE FROM OperationAllowed WHERE metadataId=?", Integer.parseInt(id));
		addPrivileges(id, localGroups, dbms);

		dbms.execute("DELETE FROM MetadataCateg WHERE metadataId=?", Integer.parseInt(id));
		addCategories(id, localCateg, dbms);

		dbms.commit();
		dataMan.indexMetadata(dbms, id);
	}
	/**
	 * Inserts a metadata into the database. Lucene index is updated after insertion.
	 * @param xml
	 * @param uuid
	 * @param dbms
	 * @param schema
	 * @param localGroups
	 * @param localCateg
	 * @throws Exception
	 */
	private String addMetadata(Element xml, String uuid, Dbms dbms, String schema, GroupMapper localGroups, CategoryMapper localCateg) throws Exception {
		System.out.println("  - Adding metadata with remote uuid: "+ uuid);

        //
        // insert metadata
        //
        String source = params.uuid;
        String createDate = new ISODate().toString();
        int userId = 1;
        String docType = null, title = null, isTemplate = null, group = null, category = null;
        boolean ufo = false, indexImmediate = false;
        String id = dataMan.insertMetadata(context, dbms, schema, xml, context.getSerialFactory().getSerial(dbms, "Metadata"), uuid, userId, group, source,
                         isTemplate, docType, title, category, createDate, createDate, ufo, indexImmediate);



		int iId = Integer.parseInt(id);
		dataMan.setTemplateExt(dbms, iId, "n", null);
		dataMan.setHarvestedExt(dbms, iId, source);

		addPrivileges(id, localGroups, dbms);
		addCategories(id, localCateg, dbms);

		dbms.commit();
		dataMan.indexMetadata(dbms, id);
		return id;
	}
	
	//--------------------------------------------------------------------------
	//--- Categories
	//--------------------------------------------------------------------------

	private void addCategories(String id, CategoryMapper localCateg, Dbms dbms) throws Exception {
		for(String catId : params.getCategories()) {
			String name = localCateg.getName(catId);

			if (name == null) {
				System.out.println("    - Skipping removed category with id:"+ catId);
			}
			else {
				System.out.println("    - Setting category : "+ name);
				dataMan.setCategory(context, dbms, id, catId);
			}
		}
	}	
	//--------------------------------------------------------------------------
	//--- Privileges
	//--------------------------------------------------------------------------

	private void addPrivileges(String id, GroupMapper localGroups, Dbms dbms) throws Exception {
		for (Privileges priv : params.getPrivileges()) {
			String name = localGroups.getName(priv.getGroupId());
			if (name == null) {
				System.out.println("    - Skipping removed group with id:"+ priv.getGroupId());
			}
			else {
				System.out.println("    - Setting privileges for group : "+ name);
				for (int opId: priv.getOperations()) {
					name = dataMan.getAccessManager().getPrivilegeName(opId);
					//--- allow only: view, dynamic, featured
					if (opId == 0 || opId == 5 || opId == 6) {
						System.out.println("       --> "+ name);
						dataMan.setOperation(context, dbms, id, priv.getGroupId(), opId +"");
					}
					else {
						System.out.println("       --> "+ name +" (skipped)");
					}
				}
			}
		}
	}
	
	@Override
	protected void doInit(Element entry) throws BadInputEx {
		params = new ArcSDEParams(dataMan);
		params.create(entry);
	}

	@Override
	protected void doUpdate(Dbms dbms, String id, Element node) throws BadInputEx, SQLException {
		ArcSDEParams copy = params.copy();

		//--- update variables
		copy.update(node);

		String path = "harvesting/id:"+ id;

		settingMan.removeChildren(dbms, path);

		//--- update database
		storeNode(dbms, copy, path);

		//--- we update a copy first because if there is an exception ArcSDEParams
		//--- could be half updated and so it could be in an inconsistent state

		Lib.sources.update(dbms, copy.uuid, copy.name, true);
		Resources.copyLogo(context, "images" + File.separator + "harvesting" + File.separator + params.icon, params.uuid);
		
		params = copy;
	}

	@Override
	public AbstractParams getParams() {
		return params;
	}

	@Override
	public String getType() {
		return "arcsde";
	}

	class ArcSDEResult {
		public int total;
		public int added;
		public int updated;
		public int removed;
		public int unknownSchema;
		public int badFormat;
		public int doesNotValidate;		
	}
}
