package org.linkeddatafragments.datasource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        final int subjectId = subject == null ? 0
                : dictionary.getIntID(subject.asNode(),
                        TripleComponentRole.SUBJECT);
        final int predicateId = predicate == null ? 0
                : dictionary.getIntID(predicate.asNode(),
                        TripleComponentRole.PREDICATE);
        final int objectId = object == null ? 0
                : dictionary.getIntID(object.asNode(),
                        TripleComponentRole.OBJECT);
        if (subjectId < 0 || predicateId < 0 || objectId < 0)
        {
            return new TriplePatternFragmentBase();
        }
        final Model triples = ModelFactory.createDefaultModel();
        final IteratorTripleID matches = datasource.getTriples()
                .search(new TripleID(subjectId, predicateId, objectId));
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
                for (int i = 0; !(atOffset = i == offset)
                        && matches.hasNext(); i++)
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
        final long estimatedTotal = triples.size() > 0
                ? Math.max(offset + triples.size() + 1,
                        matches.estimatedNumResults())
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
        return new Triple(
                dictionary.getNode(tripleId.getSubject(),
                        TripleComponentRole.SUBJECT),
                dictionary.getNode(tripleId.getPredicate(),
                        TripleComponentRole.PREDICATE),
                dictionary.getNode(tripleId.getObject(),
                        TripleComponentRole.OBJECT));
    }

    @Override
    public TriplePatternFragment getBindingFragment(
            final TripleElement _subject, final TripleElement _predicate,
            final TripleElement _object, final long offset, final long limit,
            final List<Map<Integer,Node>> solutionMappings)
    {
        // Translate the given Node objects based 'solutionMappings' into Maps
        // that map to the HDT identifiers of the Node objects. Doing this
        // translation upfront is an optimization because it avoids repeating
        // the same HDT dictionary lookups over and over again.
        final List<Map<Integer, Integer>> solmapsWithHdtIDs = new LinkedList<Map<Integer, Integer>>();
        for ( Map<Integer,Node> solmap : solutionMappings )
        {
            final Map<Integer, Integer> solmapWithHdtIDs = new HashMap<Integer, Integer>();
            for ( Map.Entry<Integer,Node> binding : solmap.entrySet() )
            {
                final Node n = binding.getValue();

                int id = dictionary.getIntID(n,
                        TripleComponentRole.SUBJECT);

                final int idP = dictionary.getIntID(n,
                        TripleComponentRole.PREDICATE);
                if (idP != -1)
                {
                    if (id != -1 && id != idP)
                        throw new IllegalStateException();

                    id = idP;
                }

                final int idO = dictionary.getIntID(n,
                        TripleComponentRole.OBJECT);
                if (idO != -1)
                {
                    if (id != -1 && id != idO)
                        throw new IllegalStateException();

                    id = idO;
                }

                solmapWithHdtIDs.put( binding.getKey(), id );
            }

            solmapsWithHdtIDs.add(solmapWithHdtIDs);
        }

        return getBindingFragmentByTriplePatternSubstitution(
                // return getBindingFragmentByTestingHdtMatches(
                _subject, _predicate, _object, offset, limit, solmapsWithHdtIDs);
    }

    public TriplePatternFragment getBindingFragmentByTriplePatternSubstitution(
            final TripleElement _subject, final TripleElement _predicate,
            final TripleElement _object, final long offset, final long limit,
            final List<Map<Integer,Integer>> solutionMappings)
    {
        // Translate the given Jena Binding objects into Maps that map
        // variables (Jena Var objects) to the HDT identifiers of the
        // corresponding RDF terms (Jena Node objects) that the Binding
        // objects bind to the variables.
        // Doing this translation upfront is an optimization because it
        // avoids repeating the same HDT dictionary lookups over and over
        // again.
        // look up the result from the HDT datasource
        int subjectId = 0;
        int predicateId = 0;
        int objectId = 0;
        if ( _subject.isNode )
        {
            subjectId = dictionary.getIntID(_subject.rdfNode,
                            TripleComponentRole.SUBJECT);
        }
        if ( _predicate.isNode )
        {
            predicateId = dictionary.getIntID(
                            _predicate.rdfNode,
                            TripleComponentRole.PREDICATE);
        }
        if ( _object.isNode )
        {
            objectId = dictionary.getIntID(_object.rdfNode,
                            TripleComponentRole.OBJECT);
        }
        final Model triples = ModelFactory.createDefaultModel();
        int triplesCheckedSoFar = 0;
        int triplesAddedInCurrentPage = 0;
        boolean atOffset;
        int bindingsSize = solutionMappings.size();
        int countBindingsSoFar = 0;
        for ( Map<Integer,Integer> solmap : solutionMappings )
        {
            int bindingsSubjectId = subjectId;
            int bindinsgPredicateId = predicateId;
            int bindingsObjectId = objectId;

            if ( _subject.isVar )
            {
                final Integer id = solmap.get( _subject.varId );
                if ( id != null ) {
                    bindingsSubjectId = id;
                }
            }
            if ( _predicate.isVar )
            {
                final Integer id = solmap.get( _predicate.varId );
                if ( id != null ) {
                    bindinsgPredicateId = id;
                }
            }
            if ( _object.isVar )
            {
                final Integer id = solmap.get( _object.varId );
                if ( id != null ) {
                    bindingsObjectId = id;
                }
            }

            final IteratorTripleID matches = datasource.getTriples()
                    .search(new TripleID(bindingsSubjectId, bindinsgPredicateId,
                            bindingsObjectId));
            final boolean hasMatches = matches.hasNext();
            if (hasMatches)
            {
                matches.goToStart();
                while (!(atOffset = (triplesCheckedSoFar == offset))
                        && matches.hasNext())
                {
                    matches.next();
                    triplesCheckedSoFar++;
                }
                // try to add `limit` triples to the result model
                if (atOffset)
                {
                    while (triplesAddedInCurrentPage < limit
                            && matches.hasNext())
                    {
                        triples.add(
                                triples.asStatement(toTriple(matches.next())));
                        triplesAddedInCurrentPage++;
                    }
                }
            }
            countBindingsSoFar++;
        }

        final long minimumTotal = offset + triplesAddedInCurrentPage + 1;
        final long estimatedTotal;
        if (triplesAddedInCurrentPage < limit)
        {
            estimatedTotal = offset + triplesAddedInCurrentPage;
        }
//         else // This else block is for testing purposes only. The next else block is the correct one.
//         {
//             estimatedTotal = minimumTotal;
//         }
        else
        {
            final int THRESHOLD = 10;
            final int maxBindingsToUseInEstimation;
            if (bindingsSize <= THRESHOLD)
            {
                maxBindingsToUseInEstimation = bindingsSize;
            } else{
                maxBindingsToUseInEstimation = THRESHOLD;
            }

            long estimationSum = 0L;
            for (int i = 0; i < maxBindingsToUseInEstimation; i++)
            {
                estimationSum += estimateResultSetSize(
                        solutionMappings.get(i), _subject, _predicate, _object, subjectId,
                        predicateId, objectId);
            }

            if (bindingsSize <= THRESHOLD)
            {
                if ( estimationSum <= minimumTotal )
                    estimatedTotal = minimumTotal;
                else
                    estimatedTotal = estimationSum;
            }
            else // bindingsSize > THRESHOLD
            {
                final double fraction = bindingsSize / maxBindingsToUseInEstimation;
                final double estimationAsDouble = fraction * estimationSum;
                final long estimation = Math.round(estimationAsDouble);
                if ( estimation <= minimumTotal )
                    estimatedTotal = minimumTotal;
                else
                    estimatedTotal = estimation;
            }
        }

        final long estimatedValid = estimatedTotal;

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
                // return estimatedpageTotal;
                return estimatedValid;
            }

        };
    }

    private long estimateResultSetSize(final Map<Integer,Integer> solmap,
            final TripleElement _subject, final TripleElement _predicate,
            final TripleElement _object, int subjectId, int predicateId,
            int objectId)
    {
        if ( _subject.isVar )
        {
            final Integer id = solmap.get( _subject.varId );
            if ( id != null ) {
                subjectId = id;
            }
        }
        if ( _predicate.isVar )
        {
            final Integer id = solmap.get( _predicate.varId );
            if ( id != null ) {
                predicateId = id;
            }
        }
        if ( _object.isVar )
        {
            final Integer id = solmap.get( _object.varId );
            if ( id != null ) {
                objectId = id;
            }
        }

        final IteratorTripleID matches = datasource.getTriples()
                .search(new TripleID(subjectId, predicateId, objectId));

        if ( matches.hasNext() )
            return Math.max(matches.estimatedNumResults(), 1);
        else
            return 0L;
    }

//     public TriplePatternFragment getBindingFragmentByTestingHdtMatches(
//             final TripleElement _subject, final TripleElement _predicate,
//             final TripleElement _object, final long offset, final long limit,
//             final List<Binding> bindings)
//     {
//         // Translate the given Jena Binding objects into Maps that map
//         // variables (Jena Var objects) to the HDT identifiers of the
//         // corresponding RDF terms (Jena Node objects) that the Binding
//         // objects bind to the variables.
//         // Doing this translation upfront is an optimization because it
//         // avoids repeating the same HDT dictionary lookups over and over
//         // again.
//         final List<Map<Var, Integer>> solmapsWithHdtIDs = new LinkedList<Map<Var, Integer>>();
//         for (Binding solmap : bindings)
//         {
//             final Map<Var, Integer> solmapWithHdtIDs = new HashMap<Var, Integer>();
//             final Iterator<Var> it = solmap.vars();
//             while (it.hasNext())
//             {
//                 final Var var = it.next();
// 
//                 int id = dictionary.getIntID(solmap.get(var),
//                         TripleComponentRole.SUBJECT);
// 
//                 final int idP = dictionary.getIntID(solmap.get(var),
//                         TripleComponentRole.PREDICATE);
//                 if (idP != -1)
//                 {
//                     if (id != -1 && id != idP)
//                         throw new IllegalStateException();
// 
//                     id = idP;
//                 }
// 
//                 final int idO = dictionary.getIntID(solmap.get(var),
//                         TripleComponentRole.OBJECT);
//                 if (idO != -1)
//                 {
//                     if (id != -1 && id != idO)
//                         throw new IllegalStateException();
// 
//                     id = idO;
//                 }
// 
//                 solmapWithHdtIDs.put(var, id);
//             }
// 
//             solmapsWithHdtIDs.add(solmapWithHdtIDs);
//         }
// 
//         return getBindingFragmentInHDT(_subject, _predicate, _object, offset,
//                 limit, solmapsWithHdtIDs);
//     }

    public TriplePatternFragment getBindingFragmentInHDT(
            final TripleElement _subject, final TripleElement _predicate,
            final TripleElement _object, final long offset, final long limit,
            final List<Map<Var, Integer>> solmapsWithHdtIDs)
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
            subjectId = _subject.object == null ? 0
                    : dictionary.getIntID(((Resource) _subject.object).asNode(),
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
            predicateId = _predicate == null ? 0
                    : dictionary.getIntID(
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
            objectId = _object.object == null ? 0
                    : dictionary.getIntID(((RDFNode) _object.object).asNode(),
                            TripleComponentRole.OBJECT);
        }
        if (subjectId < 0 || predicateId < 0 || objectId < 0)
        {
            return new TriplePatternFragmentBase();
        }
        final Model triples = ModelFactory.createDefaultModel();
        final IteratorTripleID matches = datasource.getTriples()
                .search(new TripleID(subjectId, predicateId, objectId));
        final boolean hasMatches = matches.hasNext();

        // prepare for repeatedly calling the isValid method
        final Var subjectVar = (_subject.name.equals("Var"))
                ? (Var) _subject.object : null;
        final Var predicateVar = (_predicate.name.equals("Var"))
                ? (Var) _predicate.object : null;
        final Var objectVar = (_object.name.equals("Var"))
                ? (Var) _object.object : null;

        int testedMatchesSoFar = 0; // everything from 'matches' that we have
                                    // looked at so far
        int validMatchesSoFar = 0; // the subset of 'testedMatchesSoFar' that
                                   // turned out to be valid for the brTPF
        int testedMatchesUntilFirstPageOfValidMatches = 0;
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
                if (validMatchesSoFar >= offset)
                {
                    break;
                }

                TripleID tripleId = matches.next();
                if (isValid(tripleId, solmapsWithHdtIDs, subjectVar,
                        predicateVar, objectVar, dictionary))
                {
                    validMatchesSoFar++;
                }

                testedMatchesSoFar++;
                if (validMatchesSoFar == limit)
                {
                    testedMatchesUntilFirstPageOfValidMatches = testedMatchesSoFar;
                }
            }

            // now we are at the correct offset; add `limit` triples to the
            // result model
            // (again, ignoring matching triples that are not compatible with
            // any of the
            // solution mappings in 'bindings')

            int validMatchesForRequestedPage = 0;
            while (validMatchesForRequestedPage < limit && matches.hasNext())
            {
                TripleID tripleId = matches.next();
                if (isValid(tripleId, solmapsWithHdtIDs, subjectVar,
                        predicateVar, objectVar, dictionary))
                {
                    triples.add(triples.asStatement(toTriple(tripleId)));
                    validMatchesSoFar++;
                    validMatchesForRequestedPage++;
                }
                testedMatchesSoFar++;
            }
        }

        // at this point it holds that: offset <= validMatchesSoFar <= offset +
        // limit
        final long estimatedValid;
        if (validMatchesSoFar == 0)
        {
            estimatedValid = 0L;
        }
        else if (validMatchesSoFar < limit)
        {
            estimatedValid = validMatchesSoFar;
        }
        else
        {
            final long estimatedMatches = matches.estimatedNumResults();
            if (testedMatchesUntilFirstPageOfValidMatches > 0)
                estimatedValid = (limit * estimatedMatches)
                        / testedMatchesUntilFirstPageOfValidMatches;
            else
                estimatedValid = (limit * estimatedMatches)
                        / testedMatchesSoFar;
        }

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
                return estimatedValid;
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
            final List<Map<Var, Integer>> solmapsWithHdtIDs,
            final Var subjectVar, final Var predicateVar, final Var objectVar,
            final NodeDictionary dictionary)
    {
        for (Map<Var, Integer> solMapWithHdtIDs : solmapsWithHdtIDs)
        {
            if (checkCompatibility(tripleId, solMapWithHdtIDs, subjectVar,
                    predicateVar, objectVar, dictionary))
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
            final Map<Var, Integer> solMapWithHdtIDs, final Var subjectVar,
            final Var predicateVar, final Var objectVar,
            final NodeDictionary dictionary)
    {
        if (subjectVar != null && solMapWithHdtIDs.containsKey(subjectVar))
        {
            final int a = tripleId.getSubject();
            final int b = solMapWithHdtIDs.get(subjectVar);
            if (a != b)
            {
                return false;
            }
        }

        if (predicateVar != null && solMapWithHdtIDs.containsKey(predicateVar))
        {
            final int a = tripleId.getPredicate();
            final int b = solMapWithHdtIDs.get(predicateVar);
            if (a != b)
            {
                return false;
            }
        }

        if (objectVar != null && solMapWithHdtIDs.containsKey(objectVar))
        {
            final int a = tripleId.getObject();
            final int b = solMapWithHdtIDs.get(objectVar);
            if (a != b)
            {
                return false;
            }
        }

        return true;
    }
}