package io.quantumdb.core.schema.definitions;

import io.quantumdb.core.utils.RandomHasher;
import lombok.Data;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

@Data
public class Unique {

    private transient final String uniqueName;

    private final List<String> columns;

    private Table parent;

    public Unique(List<String> columns) {
        this("unique_" + RandomHasher.generateHash(), columns);
    }

    public Unique(String uniqueName, List<String> columns) {
        checkArgument(!isNullOrEmpty(uniqueName), "You must specify a 'uniqueName'.");
        checkArgument(columns != null && !columns.isEmpty(), "You must specify at least one column.");

        this.uniqueName = uniqueName;
        this.columns = columns;
    }

    @Override
    public String toString() {
        return PrettyPrinter.prettyPrint(this);
    }

}