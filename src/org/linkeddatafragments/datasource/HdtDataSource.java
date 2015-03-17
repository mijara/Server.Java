package org.linkeddatafragments.datasource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.linkeddatafragments.util.TripleElement;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.Expr;

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
        Resource subject = (Resource) _subject.object;
        Property predicate = (Property) _predicate.object;
        RDFNode object = (RDFNode) _object.object;

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
        final IteratorTripleID result = datasource.getTriples().search(
                new TripleID(subjectId, predicateId, objectId));
        // estimates can be wrong; ensure 0 is returned if and only if there are
        // no results
        final long totalSize = result.hasNext() ? Math.max(
                result.estimatedNumResults(), 1) : 0;

        // create the fragment
        return new TriplePatternFragment()
        {
            @Override
            public Model getTriples()
            {
                final Model triples = ModelFactory.createDefaultModel();

                // try to jump directly to the offset
                boolean atOffset;
                if (result.canGoTo())
                {
                    try
                    {
                        result.goTo(offset);
                        atOffset = true;
                    } // if the offset is outside the bounds, this page has no
                      // matches
                    catch (IndexOutOfBoundsException exception)
                    {
                        atOffset = false;
                    }
                } // if not possible, advance to the offset iteratively
                else
                {
                    result.goToStart();
                    for (int i = 0; !(atOffset = i == offset)
                            && result.hasNext(); i++)
                    {
                        result.next();
                    }
                }

                // add `limit` triples to the result model
                if (atOffset)
                {
                    for (int i = 0; i < limit && result.hasNext(); i++)
                    {
                        triples.add(triples.asStatement(toTriple(result.next())));
                    }
                }
                return triples;
            }

            @Override
            public long getTotalSize()
            {
                return totalSize;
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
            subjectId = dictionary.getIntID(((Var) _subject.object),
                    TripleComponentRole.SUBJECT);
        }
        else
        {
            subjectId = _subject.object == null ? 0 : dictionary.getIntID(
                    ((Resource) _subject.object).asNode(),
                    TripleComponentRole.SUBJECT);
        }
        if (_predicate.name.equals("Var"))
        {
            predicateId = dictionary.getIntID(((Var) _predicate.object),
                    TripleComponentRole.SUBJECT);
        }
        else
        {
            predicateId = _predicate == null ? 0 : dictionary.getIntID(
                    ((RDFNode) _predicate.object).asNode(),
                    TripleComponentRole.PREDICATE);
        }
        if (_object.name.equals("Var"))
        {
            objectId = dictionary.getIntID(((Var) _object.object),
                    TripleComponentRole.SUBJECT);
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
        final IteratorTripleID result = datasource.getTriples().search(
                new TripleID(subjectId, predicateId, objectId));
        // estimates can be wrong; ensure 0 is returned if and only if there are
        // no results
        final long totalSize = result.hasNext() ? Math.max(
                result.estimatedNumResults(), 1) : 0;
        System.out.println("estimated size: " + totalSize);

        // create the fragment
        return new TriplePatternFragment()
        {
            @Override
            public Model getTriples()
            {
                final Model triples = ModelFactory.createDefaultModel();
                System.out.println(bindings);

                // try to jump directly to the offset
                boolean atOffset;
                int j = 0;
                while (result.hasNext())
                {
                    if (j >= offset)
                    {
                        break;
                    }
                    else
                    {
                        TripleID tripleId = result.next();
                        for (Binding binding : bindings)
                        {
                            boolean increase = false;
                            // Bindings: (?X ?Y) {(val1 val2)}
                            if (_subject.name.equals("Var"))
                            {
                                if (binding.contains((Var) _subject.object))
                                {
                                    if (dictionary
                                            .getNode(tripleId.getSubject(),
                                                    TripleComponentRole.SUBJECT)
                                            .equals(binding
                                                    .get((Var) _subject.object)))
                                    {
                                        increase = true;
                                    }
                                }
                            }
                            if (_predicate.name.equals("Var"))
                            {
                                if (binding.contains((Var) _predicate.object))
                                {
                                    if (dictionary
                                            .getNode(
                                                    tripleId.getSubject(),
                                                    TripleComponentRole.PREDICATE)
                                            .equals(binding
                                                    .get((Var) _predicate.object)))
                                    {
                                        increase = true;
                                    }
                                }
                            }
                            if (_object.name.equals("Var"))
                            {
                                if (binding.contains((Var) _object.object))
                                {
                                    if (dictionary.getNode(
                                            tripleId.getSubject(),
                                            TripleComponentRole.OBJECT).equals(
                                            binding.get((Var) _object.object)))
                                    {
                                        increase = true;
                                    }
                                }
                            }
                            if (increase)
                            {
                                j++;
                            }
                        }

                    }
                }
                // now I'm at offset in result, I do not need to goto
                atOffset = true;
                // if (result.canGoTo())
                // {
                // try
                // {
                // result.goTo(offset);
                // atOffset = true;
                // } // if the offset is outside the bounds, this page has no
                // // matches
                // catch (IndexOutOfBoundsException exception)
                // {
                // atOffset = false;
                // }
                // } // if not possible, advance to the offset iteratively
                // else
                // {
                // result.goToStart();
                // for (int i = 0; !(atOffset = i == offset)
                // && result.hasNext(); i++)
                // {
                // result.next();
                // }
                // }

                // add `limit` triples to the result model
                if (atOffset)
                {
                    for (int i = 0; i < limit && result.hasNext(); i++)
                    {
                        TripleID tripleId = result.next();
                        System.out.println(dictionary.getNode(
                                tripleId.getSubject(),
                                TripleComponentRole.SUBJECT).toString()
                                + " "
                                + dictionary.getNode(tripleId.getPredicate(),
                                        TripleComponentRole.PREDICATE)
                                        .toString()
                                + " "
                                + dictionary.getNode(tripleId.getObject(),
                                        TripleComponentRole.OBJECT).toString());
                        TripleID tpId = new TripleID(-1, -1, -1);
                        for (Binding binding : bindings)
                        {
                            // Bindings: (?X ?Y) {(val1 val2)}
                            if (_subject.name.equals("Var"))
                            {
                                if (binding.contains((Var) _subject.object))
                                {
                                    if (dictionary
                                            .getNode(tripleId.getSubject(),
                                                    TripleComponentRole.SUBJECT)
                                            .equals(binding
                                                    .get((Var) _subject.object)))
                                    {
                                        tpId.setSubject(tripleId.getSubject());
                                    }
                                }
                                else
                                {
                                    // UNDEF
                                    tpId.setSubject(tripleId.getSubject());
                                }
                            }
                            else
                            {
                                tpId.setSubject(tripleId.getSubject());
                            }
                            if (_predicate.name.equals("Var"))
                            {
                                if (binding.contains((Var) _predicate.object))
                                {
                                    if (dictionary
                                            .getNode(
                                                    tripleId.getPredicate(),
                                                    TripleComponentRole.PREDICATE)
                                            .equals(binding
                                                    .get((Var) _predicate.object)))
                                    {
                                        tpId.setPredicate(tripleId
                                                .getPredicate());
                                    }
                                }
                                else
                                {
                                    // UNDEF
                                    tpId.setPredicate(tripleId.getPredicate());
                                }
                            }
                            else
                            {
                                tpId.setPredicate(tripleId.getPredicate());
                            }
                            if (_object.name.equals("Var"))
                            {
                                if (binding.contains((Var) _object.object))
                                {
                                    if (dictionary.getNode(
                                            tripleId.getObject(),
                                            TripleComponentRole.OBJECT).equals(
                                            binding.get((Var) _object.object)))
                                    {
                                        tpId.setObject(tripleId.getObject());
                                    }
                                }
                                else
                                {
                                    // UNDEF
                                    tpId.setObject(tripleId.getObject());
                                }
                            }
                            else
                            {
                                tpId.setObject(tripleId.getObject());
                            }
                            if (tpId.isValid())
                            {
                                // System.out.println(tpId);
                                triples.add(triples.asStatement(toTriple(tpId)));
                                break;
                            }
                        }
                    }
                    // for (int i = 0; i < limit && result.hasNext(); i++)
                    // {
                    // triples.add(triples.asStatement(toTriple(result.next())));
                    // }
                }
                return triples;
            }

            @Override
            public long getTotalSize()
            {
                return totalSize;
            }
        };
    }

    @Override
    public TriplePatternFragment getFilterFragment(TripleElement subject,
            TripleElement predicate, TripleElement object, long offset,
            long limit, Expr expr)
    {
        // if (offset < 0)
        // {
        // throw new IndexOutOfBoundsException("offset");
        // }
        // if (limit < 1)
        // {
        // throw new IllegalArgumentException("limit");
        // }
        //
        // // look up the result from the HDT datasource
        // final int subjectId = subject == null ? 0 : dictionary.getIntID(
        // subject.asNode(), TripleComponentRole.SUBJECT);
        // final int predicateId = predicate == null ? 0 : dictionary.getIntID(
        // predicate.asNode(), TripleComponentRole.PREDICATE);
        // final int objectId = object == null ? 0 : dictionary.getIntID(
        // object.asNode(), TripleComponentRole.OBJECT);
        // if (subjectId < 0 || predicateId < 0 || objectId < 0)
        // {
        // return new TriplePatternFragmentBase();
        // }
        // final IteratorTripleID result = datasource.getTriples().search(
        // new TripleID(subjectId, predicateId, objectId));
        // // estimates can be wrong; ensure 0 is returned if and only if there
        // are
        // // no results
        // final long totalSize = result.hasNext() ? Math.max(
        // result.estimatedNumResults(), 1) : 0;
        //
        // // create the fragment
        // return new TriplePatternFragment()
        // {
        // @Override
        // public Model getTriples()
        // {
        // final Model triples = ModelFactory.createDefaultModel();
        //
        // // try to jump directly to the offset
        // boolean atOffset;
        // if (result.canGoTo())
        // {
        // try
        // {
        // result.goTo(offset);
        // atOffset = true;
        // } // if the offset is outside the bounds, this page has no
        // // matches
        // catch (IndexOutOfBoundsException exception)
        // {
        // atOffset = false;
        // }
        // } // if not possible, advance to the offset iteratively
        // else
        // {
        // result.goToStart();
        // for (int i = 0; !(atOffset = i == offset)
        // && result.hasNext(); i++)
        // {
        // result.next();
        // }
        // }
        //
        // // add `limit` triples to the result model
        // if (atOffset)
        // {
        // for (int i = 0; i < limit && result.hasNext(); i++)
        // {
        // triples.add(triples.asStatement(toTriple(result.next())));
        // }
        // }
        // return triples;
        // }
        //
        // @Override
        // public long getTotalSize()
        // {
        // return totalSize;
        // }
        // };
        return null;
    }

    // @Override
    // public TriplePatternFragment getFilterFragment(TripleElement subject,
    // TripleElement predicate, TripleElement object, long offset,
    // long limit, Expr expr)
    // {
    // // TODO Auto-generated method stub
    // return null;
    // }
}
