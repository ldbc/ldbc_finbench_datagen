package ldbc.finbench.datagen.entities;

public interface DynamicActivity {

    long getCreationDate();

    long getDeletionDate();

    boolean isExplicitlyDeleted();

}
