package org.linkeddatafragments.util;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.core.Var;

public class TripleElement
{
    public final String name;
    public final Object object;

    public final boolean isVar;
    public final String varName;
    public final Node rdfNode;

    public TripleElement(String name, Object object)
    {
        this.name = name;
        this.object = object;

        if ( name.equals("Var") ) {
            isVar = true;
            varName = ( (Var) object ).getName();
            rdfNode = null;
        }
        else if ( name.equals("RDFNode") ) {
            isVar = false;
            varName = null;
            rdfNode = ( (RDFNode) object ).asNode();
        }
        else {
            isVar = false;
            varName = null;
            rdfNode = null;
        }
    }

}
