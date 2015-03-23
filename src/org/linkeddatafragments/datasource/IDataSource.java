package org.linkeddatafragments.datasource;

import java.util.List;

import org.linkeddatafragments.util.TripleElement;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.Expr;

/**
 * A data source of Basic Linked Data Fragments.
 * @author Ruben Verborgh
 */
public interface IDataSource {
	/**
	 * Gets a page of the Basic Linked Data Fragment matching the specified triple pattern.
	 * @param subject the subject (null to match any subject)
	 * @param predicate the predicate (null to match any predicate)
	 * @param object the object (null to match any object)
	 * @param offset the triple index at which to start the page
	 * @param limit the number of triples on the page
	 * @return the first page of the fragment
	 */
	public TriplePatternFragment getFragment(TripleElement subject, TripleElement predicate, TripleElement object,
											   long offset, long limit);
        public String getTitle();
        
        public String getDescription();
        
        public TriplePatternFragment getBindingFragment(TripleElement subject, TripleElement predicate, TripleElement object,
                long offset, long limit, List<Binding> bindings);
}
