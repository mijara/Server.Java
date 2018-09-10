package org.linkeddatafragments.servlet;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that responds with a Basic Linked Data Fragment.
 * 
 * @author Ruben Verborgh
 */
public class CacheStatsTPFServlet extends TriplePatternFragmentServlet 
{
    private static AtomicLong requestsCounter = new AtomicLong( 0L );
    protected final static Queue<String> cache = new ConcurrentLinkedQueue<String> ();
    protected final static Set<String> cachedRequests = new HashSet<String> ();
    protected static AtomicLong cacheHitsCounter = new AtomicLong( 0L );

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException
    {
try {
        final String cacheKey;
        if ( request.getQueryString() == null )
            cacheKey = request.getRequestURI();
        else
            cacheKey = request.getRequestURI() + "?" + request.getQueryString();

        if ( request.getQueryString() != null && request.getQueryString().equals("cacheStats") )
        {
            String msg = "" + requestsCounter.get();
            msg += ", " + cacheHitsCounter.get();
            msg += ", " + cache.size();

System.out.println( msg );
            try {
                response.getWriter().write( msg );
                response.getWriter().flush();
            }
            catch ( Exception e ) {
            }
        }
        else if ( request.getQueryString() != null && request.getQueryString().equals("clearCache") )
        {
            clearCache();
            String msg = "cache cleared";

System.out.println( msg );
            try {
                response.getWriter().write( msg );
                response.getWriter().flush();
            }
            catch ( Exception e ) {
            }
        }
        else
        {
            requestsCounter.incrementAndGet();

            updateCacheStats( cacheKey );

            super.doGet( request, response );
        }
}
catch ( Exception ex ) {
System.err.println( ex.getClass().getName() + ": " + ex.getMessage() );
ex.printStackTrace();
}
    }

    protected void updateCacheStats( final String cacheKey )
    {
        final boolean hit;
        synchronized ( cache )
        {
            hit = cachedRequests.contains( cacheKey );

            if ( ! hit ) {
                cache.add( cacheKey );
                cachedRequests.add( cacheKey );
            }
        }

        if ( hit )
            cacheHitsCounter.incrementAndGet();
    }

    protected void clearCache()
    {
        synchronized ( cache )
        {
            cache.clear();
            cachedRequests.clear();
            requestsCounter.set( 0L );
            cacheHitsCounter.set( 0L );
        }
    }

}
