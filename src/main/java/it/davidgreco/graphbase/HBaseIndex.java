package it.davidgreco.graphbase;

import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.Index;

public class HBaseIndex<T extends Element> implements Index<T> {
    @Override
    public String getIndexName() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Class<T> getIndexClass() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Type getIndexType() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void put(String s, Object o, T t) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterable<T> get(String s, Object o) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void remove(String s, Object o, T t) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
