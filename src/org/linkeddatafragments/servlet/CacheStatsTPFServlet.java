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
 * @author Ruben Verborgh
 */
public class CacheStatsTPFServlet extends TriplePatternFragmentServlet 
{
    protected final static Set<String> cache = new TreeSet<String> ();
    protected static AtomicLong requestsCounter = new AtomicLong( 0L );
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
            String msg = "" + requestsCounter.get() + ", " + cacheHitsCounter.get();
            synchronized (cache) {
                msg += ", " + cache.size();
            }

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

            final boolean added;
            synchronized (cache) {
                added = cache.add( cacheKey );
            }

            if ( ! added )
                cacheHitsCounter.incrementAndGet();

            super.doGet( request, response );
        }
}
catch ( Exception ex ) {
System.err.println( ex.getClass().getName() + ": " + ex.getMessage() );
ex.printStackTrace();
}
    }

}
