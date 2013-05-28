<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	
	<!-- ============================================================================================= -->
	<!-- === editPanel -->
	<!-- ============================================================================================= -->

	<xsl:template name="editPanel-Arcsde">
		<div id="arcsde.editPanel">
			<xsl:call-template name="site-Arcsde"/>
			<div class="dots"/>
			<xsl:call-template name="options-Arcsde"/>
			<div class="dots"/>
			<xsl:call-template name="content-Arcsde"/>
			<div class="dots"/>
			<xsl:call-template name="privileges-Arcsde"/>
			<div class="dots"/>
			<xsl:call-template name="categories-Arcsde"/>			
			<p/>
		</div>
	</xsl:template>

	<!-- ============================================================================================= -->

	<xsl:template name="site-Arcsde">
		<h1 align="left"><xsl:value-of select="/root/gui/harvesting/site"/></h1>

		<table>
			<tr>
				<td class="padded"><xsl:value-of select="/root/gui/harvesting/name"/></td>
				<td class="padded"><input id="arcsde.name" class="content" type="text" value="" size="200"/></td>
			</tr>
			
			<tr>
				<td class="padded"><xsl:value-of select="/root/gui/harvesting/server"/></td>
				<td class="padded"><input id="arcsde.server" class="content" type="text" value="" size="300"/></td>
			</tr>
			
			<tr>
				<td class="padded"><xsl:value-of select="/root/gui/harvesting/port"/></td>
				<td class="padded"><input id="arcsde.port" class="content" type="text" value="" size="5"/></td>
			</tr>	
			
			<tr>
				<td class="padded"><xsl:value-of select="/root/gui/harvesting/username"/></td>
				<td class="padded"><input id="arcsde.username" class="content" type="text" value="" size="300"/></td>
			</tr>

			<tr>
				<td class="padded"><xsl:value-of select="/root/gui/harvesting/password"/></td>
				<td class="padded"><input id="arcsde.password" class="content" type="text" value="" size="300"/></td>
			</tr>

			<tr>
				<td class="padded"><xsl:value-of select="/root/gui/harvesting/database"/></td>
				<td class="padded"><input id="arcsde.database" class="content" type="text" value="" size="300"/></td>
			</tr>

			<tr>
				<td class="padded"><xsl:value-of select="/root/gui/harvesting/connectiontype"/></td>
				<td class="padded">
					<div>
						<input id="arcsde.radio-jdbc" name="arcsde.connectiontype" class="content" type="radio" value="jdbc" checked="checked"/>
						<label for="arcsde.radio-jdbc">JDBC</label>
					</div>
					<div>
						<input id="arcsde.radio-arcsde" name="arcsde.connectiontype" class="content" type="radio" value="arcsde"/>
						<label for="arcsde.radio-arcsde">ArcObjects SDK</label>						
					</div>
				</td>
			</tr>
            
            <tr id="jdbcOptions">
				<td class="padded"><xsl:value-of select="/root/gui/harvesting/jdbcOptions"/></td>
				<td class="padded" style="width: 300px">
                    <table>
                        <tr>
                            <td class="padded" style="white-space: nowrap">
                                <xsl:value-of select="/root/gui/harvesting/jdbcDriver"/>
                            </td>
                            <td class="padded">
                                <select id="arcsde.jdbcDriver" name="jdbcDriver" class="content">
                                    <option value="oracle.jdbc.OracleDriver">Oracle (thin)</option>
                                    <option value="com.microsoft.sqlserver.jdbc.SQLServerDriver">MS SQL Server</option>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td class="padded" style="white-space: nowrap">
                                <xsl:value-of select="/root/gui/harvesting/schemaVersion"/>
                            </td>
                            <td class="padded">
                                <select id="arcsde.schemaVersion" name="schemaVersion" class="content">
                                    <option value="9.x">9.x (GDB_USERMETADATA.XML)</option>
                                    <option value="10.x">10.x (GDB_ITEMS.DOCUMENATION)</option>
                                    <option value="custom"><xsl:value-of select="/root/gui/harvesting/customQueryValue"/></option>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td class="padded" style="white-space: nowrap"><xsl:value-of select="/root/gui/harvesting/customQuery"/></td>
                            <td class="padded" valign="top">
                                <input id="arcsde.customQuery" class="content" type="text" value="" size="300"/><br/>
                                <xsl:value-of select="/root/gui/harvesting/customQueryHelp"/>
                            </td>
                        </tr>
                    </table>
                </td>                
            </tr>
			
			<tr>
				<td class="padded" valign="bottom"><xsl:value-of select="/root/gui/harvesting/icon"/></td>
				<td class="padded">
					<select id="arcsde.icon" class="content" name="icon" size="1"/>
					&#xA0;
					<img id="arcsde.icon.image" src="" alt="" />
				</td>
			</tr>			
		</table>
	</xsl:template>

	<!-- ============================================================================================= -->

	<xsl:template name="options-Arcsde">
		<h1 align="left"><xsl:value-of select="/root/gui/harvesting/options"/></h1>
		<xsl:call-template name="schedule-widget">
			<xsl:with-param name="type">arcsde</xsl:with-param>
		</xsl:call-template>
	</xsl:template>

    <!-- ============================================================================================= -->

    <xsl:template name="content-Arcsde">
    <div>
        <h1 align="left"><xsl:value-of select="/root/gui/harvesting/content"/></h1>

        <table border="0">
			<tr>
                <td class="padded"><xsl:value-of select="/root/gui/harvesting/importxslt"/></td>
                <td class="padded">
                    &#160;
                    <select id="arcsde.importxslt" class="content" name="importxslt" size="1"/>
                </td>
            </tr>

            <tr>
                <td class="padded"><xsl:value-of select="/root/gui/harvesting/validate"/></td>
                <td class="padded"><input id="arcsde.validate" type="checkbox" value=""/></td>
            </tr>
        </table>
    </div>
    </xsl:template>
    

	<!-- ============================================================================================= -->

	<xsl:template name="privileges-Arcsde">
		<h1 align="left"><xsl:value-of select="/root/gui/harvesting/privileges"/></h1>
		
		<table>
			<tr>
				<td class="padded" valign="top"><xsl:value-of select="/root/gui/harvesting/groups"/></td>
				<td class="padded"><select id="arcsde.groups" class="content" size="8" multiple="on"/></td>					
				<td class="padded" valign="top">
					<div align="center">
						<button id="arcsde.addGroups" class="content" onclick="harvesting.arcsde.addGroupRow()">
							<xsl:value-of select="/root/gui/harvesting/add"/>
						</button>
					</div>
				</td>					
			</tr>
		</table>
		
		<table id="arcsde.privileges">
			<tr>
				<th class="padded"><b><xsl:value-of select="/root/gui/harvesting/group"/></b></th>
				<th class="padded"><b><xsl:value-of select="/root/gui/harvesting/oper/op[@id='0']"/></b></th>
				<th class="padded"><b><xsl:value-of select="/root/gui/harvesting/oper/op[@id='5']"/></b></th>
				<th class="padded"><b><xsl:value-of select="/root/gui/harvesting/oper/op[@id='6']"/></b></th>
				<th/>
			</tr>
		</table>
		
	</xsl:template>
	
	<!-- ============================================================================================= -->

	<xsl:template name="categories-Arcsde">
		<h1 align="left"><xsl:value-of select="/root/gui/harvesting/categories"/></h1>
		
		<select id="arcsde.categories" class="content" size="8" multiple="on"/>
	</xsl:template>
	
	<!-- ============================================================================================= -->	
	
    <xsl:template mode="selectoptions" match="day|hour|minute|dsopt">
		<option>
			<xsl:attribute name="value">
				<xsl:value-of select="."/>
			</xsl:attribute>
			<xsl:value-of select="@label"/>
		</option>
	</xsl:template>

    <!-- ============================================================================================= -->

</xsl:stylesheet>
