package org.linkeddatafragments.servlet;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that responds with a Basic Linked Data Fragment.
 * 
 * @author Olaf Hartig
 */
public class BoundedCacheStatsTPFServlet extends CacheStatsTPFServlet
{
    public static final int maxCacheSize = 250000;

    @Override
    protected void updateCacheStats( final String cacheKey )
    {
        final boolean hit;
        synchronized (cache)
        {
            hit = cachedRequests.contains( cacheKey );

            if ( hit ) {
                cache.remove( cacheKey );
            }
            else if ( cache.size() >= maxCacheSize ) {
                final String elmt = cache.poll();
                cachedRequests.remove( elmt );
            }

            cache.add( cacheKey );
            if ( ! hit ) {
                cachedRequests.add( cacheKey );
            }
        }

        if ( hit )
            cacheHitsCounter.incrementAndGet();
    }

}
