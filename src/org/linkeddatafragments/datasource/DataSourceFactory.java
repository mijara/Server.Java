package org.linkeddatafragments.datasource;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.linkeddatafragments.exceptions.DataSourceException;
import org.linkeddatafragments.exceptions.UnknownDataSourceTypeException;

/**
 *
 * @author Miel Vander Sande
 */
public class DataSourceFactory {

    public static IDataSource create(JsonObject config) throws DataSourceException {
        String title = config.getAsJsonPrimitive("type").getAsString();
        String description = config.getAsJsonPrimitive("description").getAsString();
        String type = config.getAsJsonPrimitive("type").getAsString();
        
        JsonObject settings = config.getAsJsonObject("settings");

        switch (type) {
            case "HdtDatasource":
                File file = new File(settings.getAsJsonPrimitive("file").getAsString());
                
                try {
                    return new HdtDataSource(title, description, file.getAbsolutePath());
                } catch (IOException ex) {
                    throw new DataSourceException(ex);
                }
            case "BlazegraphDataSource":
                final Properties props = new Properties();
                Iterator<Map.Entry<String,JsonElement>> it = settings.entrySet().iterator();
                while ( it.hasNext() ) {
                    final Map.Entry<String,JsonElement> entry = it.next();
                    props.setProperty( entry.getKey(), entry.getValue().getAsString() );
                }

                try {
                    return new BlazegraphDataSource( title, description, props );
                } catch ( Exception ex ) {
                    throw new DataSourceException(ex);
                }
            default:
                throw new UnknownDataSourceTypeException(type);

        }

    }

}
