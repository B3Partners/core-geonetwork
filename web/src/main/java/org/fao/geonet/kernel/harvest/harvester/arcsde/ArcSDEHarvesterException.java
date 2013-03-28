package org.fao.geonet.kernel.harvest.harvester.arcsde;

/**
 * @author heikki doeleman
 */
public class ArcSDEHarvesterException extends Exception {
    public ArcSDEHarvesterException() {
    }

    public ArcSDEHarvesterException(String message) {
        super(message);
    }

    public ArcSDEHarvesterException(String message, Throwable cause) {
        super(message, cause);
    }

    public ArcSDEHarvesterException(Throwable cause) {
        super(cause);
    }
}
