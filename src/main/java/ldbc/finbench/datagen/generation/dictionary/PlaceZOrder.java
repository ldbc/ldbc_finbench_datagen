package ldbc.finbench.datagen.generation.dictionary;

// Private class used to sort countries by their z-order value.
class PlaceZOrder implements Comparable<PlaceZOrder> {

    public int id;
    Integer zvalue;

    PlaceZOrder(int id, int zvalue) {
        this.id = id;
        this.zvalue = zvalue;
    }

    public int compareTo(PlaceZOrder obj) {
        return zvalue.compareTo(obj.zvalue);
    }
}
