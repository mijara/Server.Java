package org.linkeddatafragments.test;

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.linkeddatafragments.datasource.HdtDataSource;
import org.linkeddatafragments.datasource.TriplePatternFragment;
import org.linkeddatafragments.servlet.TriplePatternFragmentServlet;
import org.linkeddatafragments.util.TripleElement;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.Expr;

public class EvaluationTest
{
    @Test
    public void testParseAsSetOfBindings()
    {
        String bindingsString = "(?X ?Y) {(\"a hola\" 2) (<http://example.org> UNDEF) }";
        List<Binding> bindingsList = TriplePatternFragmentServlet
                .parseAsSetOfBindings(bindingsString);
        System.out.println(bindingsList.size());
        for (Binding binding : bindingsList)
        {
            System.out.println(binding);
        }
    }

    @Test
    public void testParseAsSetOfFilters()
    {
        String filtersString = "FILTER (?X = \"a hola\" && ?Y= 2)";
        Expr expr = TriplePatternFragmentServlet
                .parseAsSetOfFilters(filtersString);
        System.out.println(expr);
    }

    @Test
    public void testGetBindingFragment() throws IOException
    {
        String bindingsString = "(?X ?Y) {(<http://data.semanticweb.org/person/gary-marchionini> <http://swrc.ontoware.org/ontology#affiliation>)" +
                " (<http://data.semanticweb.org/conference/www/2010/paper/main/100> <http://purl.org/dc/elements/1.1/creator>) " +
                " (<http://data.semanticweb.org/organization/unc-chapel-hill> UNDEF) }";
        List<Binding> bindingsList = TriplePatternFragmentServlet
                .parseAsSetOfBindings(bindingsString);
        HdtDataSource datasource = new HdtDataSource("test_datasource",
                "test datasource",
                "data/swdf-2012-11-28.hdt");

        final TripleElement _subject = TriplePatternFragmentServlet
                .parseAsResource("?X");
        final TripleElement _predicate = TriplePatternFragmentServlet
                .parseAsProperty("?Y");
        final TripleElement _object = TriplePatternFragmentServlet
                .parseAsNode("<http://data.semanticweb.org/person/chi-young-oh>");

        TriplePatternFragment tpf = datasource.getBindingFragmentUsingHdtIds(_subject, _predicate, _object, 2, 4,
                bindingsList);
        System.out.println("Estimated size: " + tpf.getTotalSize());
        Model m = tpf.getTriples();
        System.out.println(m.size());
    }
}
