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
    public final int varId;

    public final boolean isNode;
    public final Node rdfNode;

    public TripleElement(String name, Object object)
    {
        this.name = name;
        this.object = object;

        isVar = false;
        varName = null;
        varId = -1;

        if ( name.equals("Var") || object instanceof Var ) {
            throw new UnsupportedOperationException();
        }
        else if ( name.equals("RDFNode") || object instanceof RDFNode ) {
            isNode = true;
            rdfNode = ( (RDFNode) object ).asNode();
        }
        else {
            isNode = false;
            rdfNode = null;
        }
    }

    public TripleElement( final String varName, final int varId )
    {
        this.name = "Var";
        this.object = Var.alloc( varName );

        isVar = true;
        this.varName = varName;
        this.varId = varId;

        isNode = false;
        rdfNode = null;
    }

    public TripleElement( RDFNode node )
    {
        this.name = "RDFNode";
        this.object = node;

        isVar = false;
        varName = null;
        varId = -1;

        isNode = true;
        rdfNode = node.asNode();
    }

}
