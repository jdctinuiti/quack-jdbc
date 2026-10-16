package com.gizmodata.quack.jdbc.sql;

import com.gizmodata.quack.jdbc.QuackProtocolException;
import com.gizmodata.quack.jdbc.type.ExtraTypeInfo;
import com.gizmodata.quack.jdbc.type.LogicalType;

import java.util.stream.Collectors;

/** SQL types for the typed prototype of a Quack insert stream. */
final class SqlTypeName {
    private SqlTypeName() {}

    static String render(LogicalType type) {
        return switch (type.id()) {
            case STRUCT -> {
                ExtraTypeInfo.StructInfo info = info(type, ExtraTypeInfo.StructInfo.class);
                yield info.childTypes().stream()
                        .map(child -> "\"" + child.name().replace("\"", "\"\"") + "\" " + render(child.type()))
                        .collect(Collectors.joining(", ", "STRUCT(", ")"));
            }
            case LIST -> render(info(type, ExtraTypeInfo.ListInfo.class).childType()) + "[]";
            case ARRAY -> {
                ExtraTypeInfo.ArrayInfo info = info(type, ExtraTypeInfo.ArrayInfo.class);
                yield render(info.childType()) + "[" + info.size() + "]";
            }
            case MAP -> {
                LogicalType entry = info(type, ExtraTypeInfo.ListInfo.class).childType();
                ExtraTypeInfo.StructInfo pair = info(entry, ExtraTypeInfo.StructInfo.class);
                if (pair.childTypes().size() != 2) throw new QuackProtocolException("Invalid MAP type");
                yield "MAP(" + render(pair.childTypes().get(0).type()) + ", "
                        + render(pair.childTypes().get(1).type()) + ")";
            }
            case ENUM -> info(type, ExtraTypeInfo.EnumInfo.class).values().stream()
                    .map(SqlLiteral::render).collect(Collectors.joining(", ", "ENUM(", ")"));
            default -> JdbcTypeMap.typeName(type);
        };
    }

    private static <T extends ExtraTypeInfo> T info(LogicalType type, Class<T> cls) {
        return type.typeInfo().filter(cls::isInstance).map(cls::cast)
                .orElseThrow(() -> new QuackProtocolException("Missing details for " + type.id()));
    }
}
