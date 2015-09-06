package org.linkeddatafragments.datasource;

import java.io.IOException;
import java.util.List;

import org.linkeddatafragments.util.TripleElement;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

/**
 * An HDT data source of Basic Linked Data Fragments.
 * 
 * @author Ruben Verborgh
 */
public class HdtDataSource extends DataSource
{

    private final HDT datasource;
    private final NodeDictionary dictionary;

    /**
     * Creates a new HdtDataSource.
     * 
     * @param title
     *            title of the datasource
     * @param description
     *            datasource description
     * @param hdtFile
     *            the HDT datafile
     * @throws IOException
     *             if the file cannot be loaded
     */
    public HdtDataSource(String title, String description, String hdtFile)
            throws IOException
    {
        super(title, description);
        datasource = HDTManager.mapIndexedHDT(hdtFile, null);
        dictionary = new NodeDictionary(datasource.getDictionary());
    }

    @Override
    public TriplePatternFragment getFragment(TripleElement _subject,
            TripleElement _predicate, TripleElement _object, final long offset,
            final long limit)
    {
        Resource subject = null;
        if (!_subject.name.equals("Var"))
        {
            subject = (Resource) _subject.object;
        }
        Property predicate = null;
        if (!_predicate.name.equals("Var"))
        {
            predicate = (Property) _predicate.object;
        }
        RDFNode object = null;
        if (!_object.name.equals("Var"))
        {
            object = (RDFNode) _object.object;
        }

        if (offset < 0)
        {
            throw new IndexOutOfBoundsException("offset");
        }
        if (limit < 1)
        {
            throw new IllegalArgumentException("limit");
        }

        // look up the result from the HDT datasource
        final int subjectId = subject == null ? 0 : dictionary.getIntID(
                subject.asNode(), TripleComponentRole.SUBJECT);
        final int predicateId = predicate == null ? 0 : dictionary.getIntID(
                predicate.asNode(), TripleComponentRole.PREDICATE);
        final int objectId = object == null ? 0 : dictionary.getIntID(
                object.asNode(), TripleComponentRole.OBJECT);
        if (subjectId < 0 || predicateId < 0 || objectId < 0)
        {
            return new TriplePatternFragmentBase();
        }
        final Model triples = ModelFactory.createDefaultModel();
        final IteratorTripleID matches = datasource.getTriples().search(
                new TripleID(subjectId, predicateId, objectId));
        final boolean hasMatches = matches.hasNext();

        if (hasMatches)
        {
            // try to jump directly to the offset
            boolean atOffset;
            if (matches.canGoTo())
            {
                try
                {
                    matches.goTo(offset);
                    atOffset = true;
                }
                // if the offset is outside the bounds, this page has no matches
                catch (IndexOutOfBoundsException exception)
                {
                    atOffset = false;
                }
            }
            // if not possible, advance to the offset iteratively
            else
            {
                matches.goToStart();
                for (int i = 0; !(atOffset = i == offset) && matches.hasNext(); i++)
                    matches.next();
            }
            // try to add `limit` triples to the result model
            if (atOffset)
            {
                for (int i = 0; i < limit && matches.hasNext(); i++)
                    triples.add(triples.asStatement(toTriple(matches.next())));
            }
        }

        // estimates can be wrong; ensure 0 is returned if there are no results,
        // and always more than actual results
        final long estimatedTotal = triples.size() > 0 ? Math.max(offset
                + triples.size() + 1, matches.estimatedNumResults())
                : hasMatches ? Math.max(matches.estimatedNumResults(), 1) : 0;

        // create the fragment
        return new TriplePatternFragment()
        {
            @Override
            public Model getTriples()
            {
                return triples;
            }

            @Override
            public long getTotalSize()
            {
                return estimatedTotal;
            }
        };
    }

    /**
     * Converts the HDT triple to a Jena Triple.
     * 
     * @param tripleId
     *            the HDT triple
     * @return the Jena triple
     */
    private Triple toTriple(TripleID tripleId)
    {
        return new Triple(dictionary.getNode(tripleId.getSubject(),
                TripleComponentRole.SUBJECT), dictionary.getNode(
                tripleId.getPredicate(), TripleComponentRole.PREDICATE),
                dictionary.getNode(tripleId.getObject(),
                        TripleComponentRole.OBJECT));
    }

    @Override
    public TriplePatternFragment getBindingFragment(
            final TripleElement _subject, final TripleElement _predicate,
            final TripleElement _object, final long offset, final long limit,
            final List<Binding> bindings)
    {
        if (offset < 0)
        {
            throw new IndexOutOfBoundsException("offset");
        }
        if (limit < 1)
        {
            throw new IllegalArgumentException("limit");
        }

        // look up the result from the HDT datasource
        int subjectId = 0;
        int predicateId = 0;
        int objectId = 0;
        if (_subject.name.equals("Var"))
        {
            subjectId = 0;
            // subjectId = dictionary.getIntID(((Var) _subject.object),
            // TripleComponentRole.SUBJECT);
        }
        else
        {
            subjectId = _subject.object == null ? 0 : dictionary.getIntID(
                    ((Resource) _subject.object).asNode(),
                    TripleComponentRole.SUBJECT);
        }
        if (_predicate.name.equals("Var"))
        {
            // predicateId = dictionary.getIntID(((Var) _predicate.object),
            // TripleComponentRole.PREDICATE);
            predicateId = 0;
        }
        else
        {
            predicateId = _predicate == null ? 0 : dictionary.getIntID(
                    ((RDFNode) _predicate.object).asNode(),
                    TripleComponentRole.PREDICATE);
        }
        if (_object.name.equals("Var"))
        {
            // objectId = dictionary.getIntID(((Var) _object.object),
            // TripleComponentRole.OBJECT);
            objectId = 0;
        }
        else
        {
            objectId = _object.object == null ? 0 : dictionary.getIntID(
                    ((RDFNode) _object.object).asNode(),
                    TripleComponentRole.OBJECT);
        }
        if (subjectId < 0 || predicateId < 0 || objectId < 0)
        {
            return new TriplePatternFragmentBase();
        }
        final Model triples = ModelFactory.createDefaultModel();
        final IteratorTripleID matches = datasource.getTriples().search(
                new TripleID(subjectId, predicateId, objectId));
        final boolean hasMatches = matches.hasNext();

        int validResults = 0;
        int checkedResults = 0;
        int j = 0;
        if (hasMatches)
        {
            matches.goToStart();
            // iterate over the matching triples until we are at the correct
            // offset
            // (ignoring matching triples that are not compatible with any of
            // the
            // solution mappings in 'bindings')
            while (matches.hasNext())
            {
                if (checkedResults >= offset)
                {
                    break;
                }

                TripleID tripleId = matches.next();
                if ( isValid(tripleId, bindings, _subject, _predicate, _object, dictionary) )
                {
                    checkedResults++;
                }

                j++;
            }

            // now we are at the correct offset; add `limit` triples to the
            // result model
            // (againg, ignoring matching triples that are not compatible with
            // any of the
            // solution mappings in 'bindings')

            validResults = 0;
            while (validResults < limit && matches.hasNext())
            {
                TripleID tripleId = matches.next();
                if ( isValid(tripleId, bindings, _subject, _predicate, _object, dictionary) )
                {
                    triples.add(triples.asStatement(toTriple(tripleId)));
                    validResults++;
                    checkedResults++;
                }
                j++;
            }
        }

        // at this point it holds that: offset <= checkedResults <= offset + limit
        long _estimatedTotal;
        if ( checkedResults > 0 )
        {
            if ( validResults < limit )
            {
                _estimatedTotal = checkedResults;
            }
            else if ( ! matches.hasNext() )
            {
                _estimatedTotal = checkedResults;
            }
            else
            {
                // in this case we have: checkedResults = offset + limit
                _estimatedTotal = Math.max( checkedResults + 1, matches.estimatedNumResults() );
            }
        }
        else
        {
            _estimatedTotal = 0;
        }
        final long estimatedTotal = _estimatedTotal;

        // create the fragment
        return new TriplePatternFragment()
        {
            @Override
            public Model getTriples()
            {
                return triples;
            }

            @Override
            public long getTotalSize()
            {
                return estimatedTotal;
            }
        };
    }

    /**
     * Assuming that tripleId is a matching triple for the triple pattern
     * (_subject,_predicate,_object), this method returns true if the solution
     * mapping that can be generated from this matching triple is compatible
     * with at least on of the solution mappings in solMapSet.
     */
    static public boolean isValid(final TripleID tripleId,
            final List<Binding> solMapSet, final TripleElement _subject,
            final TripleElement _predicate, final TripleElement _object,
            final NodeDictionary dictionary)
    {
        Var subjectVar   = ( _subject.name.equals("Var") )   ? (Var) _subject.object : null;
        Var predicateVar = ( _predicate.name.equals("Var") ) ? (Var) _predicate.object : null;
        Var objectVar    = ( _object.name.equals("Var") )    ? (Var) _object.object : null;
        
        for (Binding solMap : solMapSet)
        {
            if ( checkCompatibility(tripleId, solMap, subjectVar, predicateVar, objectVar, dictionary) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Assuming that tripleId is a matching triple for the triple pattern
     * (_subject,_predicate,_object), this method returns true if the solution
     * mapping that can be generated from this matching triple is compatible
     * with the given solution mapping (solMap).
     */
    static public boolean checkCompatibility(final TripleID tripleId,
            final Binding solMap, final Var subjectVar,
            final Var predicateVar, final Var objectVar,
            final NodeDictionary dictionary)
    {
        if (subjectVar != null && solMap.contains(subjectVar))
        {
            Node a = dictionary.getNode(tripleId.getSubject(),
                    TripleComponentRole.SUBJECT);
            Node b = solMap.get(subjectVar);
            if (!a.equals(b))
            {
                return false;
            }
        }

        if (predicateVar != null && solMap.contains(predicateVar))
        {
            Node a = dictionary.getNode(tripleId.getPredicate(),
                    TripleComponentRole.PREDICATE);
            Node b = solMap.get(predicateVar);
            if (!a.equals(b))
            {
                return false;
            }
        }

        if (objectVar != null && solMap.contains(objectVar))
        {
            Node a = dictionary.getNode(tripleId.getObject(),
                    TripleComponentRole.OBJECT);
            Node b = solMap.get(objectVar);
            if (!a.equals(b))
            {
                return false;
            }
        }

        return true;
    }
}
