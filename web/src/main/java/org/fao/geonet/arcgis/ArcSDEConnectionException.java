package org.fao.geonet.arcgis;

/**
 * @author heikki doeleman
 */
public class ArcSDEConnectionException extends Exception {
    public ArcSDEConnectionException() {
    }

    public ArcSDEConnectionException(String message) {
        super(message);
    }

    public ArcSDEConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ArcSDEConnectionException(Throwable cause) {
        super(cause);
    }
}
