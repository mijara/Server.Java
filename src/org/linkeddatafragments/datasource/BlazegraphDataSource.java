package org.linkeddatafragments.datasource;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.core.Var;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;

import com.bigdata.journal.Options;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOFilter;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.relation.accesspath.IAccessPath;

import org.linkeddatafragments.exceptions.DataSourceException;
import org.linkeddatafragments.util.TripleElement;

/**
 * A Blazegraph-based data source.
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class BlazegraphDataSource extends DataSource
{
    final protected BigdataSailRepository repo;
    final protected boolean repoHasToBeShutDown;
    final protected AbstractTripleStore store;

    public BlazegraphDataSource( final String title,
                                 final String description,
                                 final Properties props )
                                                     throws DataSourceException
    {
        this( title,
              description,
              new BigdataSailRepository(new BigdataSail(props)),
              true );  // repoHasToBeShutDown=true

        try {
            repo.initialize();
        }
        catch ( Exception e ) {
            throw new DataSourceException( e );
        }
        
//        store.init();

        if ( props.getProperty(Options.BUFFER_MODE, "dummy").equals("MemStore")
             && props.containsKey("file") )
        {
            loadData( props.getProperty("file") );
        }
    }

    public BlazegraphDataSource( final String title,
                                 final String description,
                                 final BigdataSailRepository repo )
    {
        this( title, description, repo, false ); // repoHasToBeShutDown=false
    }

    protected BlazegraphDataSource( final String title,
                                    final String description,
                                    final BigdataSailRepository repo,
                                    final boolean repoHasToBeShutDown )
    {
        super( title, description );

        this.repo = repo;
        this.repoHasToBeShutDown = repoHasToBeShutDown;
        store = repo.getDatabase();

        // Verify that the given store has properties that are
        // compatible with how we want to use the store here
        // (in particular, it must be guaranteed that matching
        // triples are always returned in the same order). 
        verifyStoreIsSuitable( store );
    }

    public void verifyStoreIsSuitable( final AbstractTripleStore store )
    {
        if ( store.isQuads() ) {
            throw new IllegalArgumentException(
                          "The given store must be for triples (not quads)." );
        }
        
        if ( store.isStatementIdentifiers() ) {
            throw new IllegalArgumentException(
                                "The given store must not support RDR/SIDS." );
        }

//        if ( ! store.isReadOnly() ) {
//            throw new IllegalArgumentException(
//                                        "The given store must be read-only." );
//        }

        if ( store.isJustify() ) {
            throw new IllegalArgumentException(
              "The given store must not support justication of entailments." );
        }
    }

    @Override
    public void close()
    {
System.out.println( "SHUTTING DOWN THE REPO" );
        if ( repoHasToBeShutDown ) {
            try {
                repo.shutDown();                
            }
            catch ( RepositoryException e ) {
                throw new IllegalStateException( e );
            }
        }
    }

    protected void loadData( String filename ) throws DataSourceException
    {
        if ( filename == null )
            return;

        final RepositoryConnection cxn;
        try {
            cxn = repo.getConnection();
            cxn.begin();
        }
        catch ( RepositoryException e ) {
            throw new DataSourceException( e );
        }

        try {
            String baseURL = "http://db.uwaterloo.ca/~galuc/wsdbm/";
            InputStream is = new FileInputStream( filename );
            final Reader reader = new InputStreamReader( new BufferedInputStream(is) );
//            cxn.add( reader, baseURL, RDFFormat.forFileName(filename) );
cxn.add( reader, baseURL, RDFFormat.TURTLE );
            cxn.commit();
        }
        catch ( Exception e ) {
            try {
                cxn.rollback();
            }
            catch ( Exception e2 ) {
                // ignore
            }
            throw new DataSourceException( e );
        }
        finally {
            // close the repository connection
            try {
                cxn.close();
            }
            catch ( Exception e2 ) {
                // ignore
            }
        }
    }

    @Override
    public TriplePatternFragment getFragment( final TripleElement subject,
                                              final TripleElement predicate,
                                              final TripleElement object,
                                              final long offset,
                                              final long limit )
    {
        final LexiconRelation lexicon = store.getLexiconRelation();
        final BlazegraphTripleElement s = convertToBlazegraphTripleElement( subject, lexicon.getValueFactory() );
        final BlazegraphTripleElement p = convertToBlazegraphTripleElement( predicate, lexicon.getValueFactory() );
        final BlazegraphTripleElement o = convertToBlazegraphTripleElement( object, lexicon.getValueFactory() );

        return getFragment( s, p, o, offset, limit );
    }

    public TriplePatternFragment getFragment( final BlazegraphTripleElement s,
                                              final BlazegraphTripleElement p,
                                              final BlazegraphTripleElement o,
                                              final long offset,
                                              final long limit )
    {
        int numOfValues = 0;
        if ( ! s.isVariable() )
            numOfValues++;
        if ( ! p.isVariable() )
            numOfValues++;
        if ( ! o.isVariable() )
            numOfValues++;

        if ( numOfValues > 0 )
        {
            final BigdataValue values[] = new BigdataValue[numOfValues];
            int i = 0;
            if ( ! s.isVariable() )
                values[i++] = s.getValue();
            if ( ! p.isVariable() )
                values[i++] = p.getValue();
            if ( ! o.isVariable() )
                values[i++] = o.getValue();

            // Initialize the IVs of the values.
            store.getLexiconRelation().addTerms( values, numOfValues, true ); // readOnly=true

            // Check if some IVs have not been initialized, which means that
            // the corresponding values are not used in any triple in the
            // store. Hence, there cannot be any matching triple for the
            // requested triple pattern and, thus, we return an empty TPF.
            for ( int j = 0; j < numOfValues; j++ )
            {
                @SuppressWarnings("rawtypes")
                final IV iv = values[j].getIV();
                if ( iv == null ||
                     (iv instanceof TermId) && ((TermId<?>) iv).getTermId()==0L )
                    return new TriplePatternFragmentBase( null, 0L );
            }
        }

        final VariablesBasedFilter filter = createFilterIfNeeded( s, p, o );
        final IAccessPath<ISPO> ap = store.getSPORelation().getAccessPath(
                                                               s.getIVorNull(),
                                                               p.getIVorNull(),
                                                               o.getIVorNull(),
                                                               null, // c=null
                                                               filter );
        final long count = ap.rangeCount( false ); // exact=false, i.e., fast range count

        if ( count == 0L ) {
            return new TriplePatternFragmentBase( null, 0L );
        }

        BigdataStatementIterator it =
                      store.asStatementIterator( ap.iterator(offset,limit,0) ); // capacity=0, i.e., default capacity will be used

        return new TriplePatternFragmentBase( consumeIntoJenaModel(it), count );
    }

    @Override
    public TriplePatternFragment getBindingFragment(
                                        final TripleElement subject,
                                        final TripleElement predicate,
                                        final TripleElement object,
                                        final long offset,
                                        final long limit,
                                        final List<Map<Integer,Node>> solMaps )
    {
        return getBindingsRestrictedTPF( subject, predicate, object,
                                         solMaps,
                                         offset, limit );
    }

    public TriplePatternFragment getBindingsRestrictedTPF(
                                           final TripleElement subject,
                                           final TripleElement predicate,
                                           final TripleElement object,
                                           final List<Map<Integer,Node>> solMaps,
                                           final long offset,
                                           final long limit )
    {
        throw new UnsupportedOperationException();
    }

    public BlazegraphTripleElement convertToBlazegraphTripleElement(
                                       final TripleElement e,
                                       final BigdataValueFactory valueFactory )
    {
        if ( e == null || e.name == null )
            throw new IllegalArgumentException();

        if ( e.name.equals("Var") )
            return new NamedVariable( (Var) e.object );

        if ( e.name == null || e.name.equals("null") || e.object == null )
            return new UnnamedVariable();

        final RDFNode rdfTerm = (RDFNode) e.object;
        if ( rdfTerm.isURIResource() ) {
            final String uri = rdfTerm.asResource().getURI();
            return new RDFTerm( valueFactory.createURI(uri) );
        }
        else if ( rdfTerm.isLiteral() ) {
            final Literal literal = rdfTerm.asLiteral();
            final String datatypeURI = literal.getDatatypeURI();
            final String languageTag = literal.getLanguage();

            final BigdataValue value;
            if ( datatypeURI != null )
                value = valueFactory.createLiteral( literal.getLexicalForm(),
                                         valueFactory.createURI(datatypeURI) );
            else if ( languageTag != null )
                value = valueFactory.createLiteral( literal.getLexicalForm(),
                                                    languageTag );
            else
                value = valueFactory.createLiteral( literal.getLexicalForm() );

            return new RDFTerm( value );
        }
        else
            throw new IllegalArgumentException();
    }

    public VariablesBasedFilter createFilterIfNeeded(
                                             final BlazegraphTripleElement s,
                                             final BlazegraphTripleElement p,
                                             final BlazegraphTripleElement o )
    {
        if (    ! s.isNamedVariable()
             && ! p.isNamedVariable()
             && ! o.isNamedVariable() )
            return null;

        final Set<String> varNames = new HashSet<String>();

        if ( s.isNamedVariable() ) {
            varNames.add( s.getVarName() );
        }

        if ( p.isNamedVariable() ) {
            if ( varNames.contains(p.getVarName()) )
                return new VariablesBasedFilter( s, p, o );

            varNames.add( p.getVarName() );
        }

        if ( o.isNamedVariable() && varNames.contains(o.getVarName()) ) {
            return new VariablesBasedFilter( s, p, o );
        }

        return null;
    }

    public Model consumeIntoJenaModel( BigdataStatementIterator it )
    {
        final Model model = ModelFactory.createDefaultModel();

        while ( it.hasNext() ) {
            model.add( convertToJenaStatement(it.next(), model) );
        }

        return model;
    }

    public Statement convertToJenaStatement( final BigdataStatement t,
                                             final Model model )
    {
        final Resource s = convertToJenaResource( t.getSubject(), model );
        final Property p = convertToJenaProperty(t.getPredicate(), model );
        final RDFNode  o = convertToJenaRDFNode(t.getObject(), model );

        return model.createStatement( s, p, o );        
    }

    public Resource convertToJenaResource( final BigdataResource r,
                                           final Model model )
    {
        return model.createResource( r.stringValue() );        
    }

    public Property convertToJenaProperty( final BigdataResource r,
                                           final Model model )
    {
        return model.createProperty( r.stringValue() );        
    }

    public RDFNode convertToJenaRDFNode( final BigdataValue v,
                                         final Model model )
    {
        if ( v instanceof BigdataResource )
            return convertToJenaResource( (BigdataResource) v, model );

        if ( !(v instanceof BigdataLiteral) )
            throw new IllegalArgumentException( v.getClass().getName() );

        final BigdataLiteral l = (BigdataLiteral) v;
        final String lex = l.getLabel();
        final URI datatypeURI = l.getDatatype();
        final String languageTag = l.getLanguage();

        if ( datatypeURI != null )
            return model.createTypedLiteral( lex, datatypeURI.stringValue() );

        else if ( languageTag != null )
            return model.createLiteral( lex, languageTag );

        else
            return model.createLiteral( lex );        
    }

    static public interface BlazegraphTripleElement
    {
        boolean isVariable();
        boolean isNamedVariable();
        String getVarName();
        BigdataValue getValue();

        @SuppressWarnings("rawtypes")
        IV getIVorNull();
    }

    static abstract public class Variable implements BlazegraphTripleElement
    {
        public boolean isVariable() { return true; }
        public BigdataValue getValue() { throw new UnsupportedOperationException(); }

        @SuppressWarnings("rawtypes")
        public IV getIVorNull() { return null; }
    }

    static public class NamedVariable extends Variable
    {
        final public String varName; 
        public NamedVariable( Var var ) { varName = var.getName(); }
        public boolean isNamedVariable() { return true; }
        public String getVarName() { return varName; }

        public String toString() { return "NamedVariable(" + varName + ")"; }
    }

    static public class UnnamedVariable extends Variable
    {
        public boolean isNamedVariable() { return false; }
        public String getVarName() { throw new UnsupportedOperationException(); }

        public String toString() { return "UnnamedVariable(" + hashCode() + ")"; } 
    }

    static public class RDFTerm implements BlazegraphTripleElement
    {
        final public BigdataValue value; 
        public RDFTerm( BigdataValue value ) { this.value = value; }
        public boolean isVariable() { return false; }
        public boolean isNamedVariable() { return false; }
        public String getVarName() { throw new UnsupportedOperationException(); }
        public BigdataValue getValue() { return value; }

        @SuppressWarnings("rawtypes")
        public IV getIVorNull() { return value.getIV(); }

        public String toString() { return "RDFTerm(" + value.toString() + ")"; } 
    }

    static public class VariablesBasedFilter extends SPOFilter<ISPO>
    {
        static final private long serialVersionUID = 6979067019748992496L;
        final public boolean checkS, checkP, checkO;

        public VariablesBasedFilter( final BlazegraphTripleElement s,
                                     final BlazegraphTripleElement p,
                                     final BlazegraphTripleElement o ) {
            boolean _checkS = false;
            boolean _checkP = false;
            boolean _checkO = false;

            if ( s.isNamedVariable() )
            {
                final String sVarName = s.getVarName();
                if ( p.isNamedVariable() && p.getVarName().equals(sVarName) ) {
                    _checkS = true; _checkP = true;
                }
                if ( o.isNamedVariable() && o.getVarName().equals(sVarName) ) {
                    _checkS = true; _checkO = true;
                }
            }

            if (    p.isNamedVariable()
                 && o.isNamedVariable()
                 && p.getVarName().equals(o.getVarName()) ) {
                _checkP = true; _checkO = true;
            }
            
            checkS = _checkS;
            checkP = _checkP;
            checkO = _checkO;
        }

        @Override
        public boolean isValid( Object obj )
        {
            if ( ! canAccept(obj) )
                return true;

            final ISPO spo = (ISPO) obj;
            final Value s = spo.getSubject();
            final Value p = spo.getPredicate();
            final Value o = spo.getObject();

            if ( checkS && checkP && ! s.equals(p) )
                return false;

            if ( checkS && checkO && ! s.equals(o) )
                return false;

            if ( checkP && checkO && ! p.equals(o) )
                return false;

            return true;
        }

    } // end of class VariablesBasedFilter

}
