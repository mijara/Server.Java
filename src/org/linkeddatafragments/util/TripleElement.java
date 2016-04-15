package org.linkeddatafragments.util;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.core.Var;

public class TripleElement
{
    public final String name;
    public final Object object;

    public final Var var;
    public final Node node;

    public TripleElement(String name, Object object)
    {
        this.name = name;
        this.object = object;

        if ( name.equals("Var") || object instanceof Var ) {
            this.var = (Var) object;
            this.node = null;
        }
        else if ( name.equals("RDFNode") || object instanceof RDFNode ) {
            this.var = null;
            this.node = ( (RDFNode) object ).asNode();
        }
        else {
            this.var = null;
            this.node = null;
        }
    }

    public TripleElement( Var var )
    {
        this.name = "Var";
        this.object = var;
        this.var = var;
        this.node = null;
    }

    public TripleElement( RDFNode node )
    {
        this.name = "RDFNode";
        this.object = node;
        this.var = null;
        this.node = node.asNode();
    }

}
