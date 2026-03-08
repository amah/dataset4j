package dataset4j.annotations.processor;

import dataset4j.annotations.DataColumn;
import dataset4j.annotations.GenerateFields;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeKind;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Annotation processor that generates static field constants for records annotated with @GenerateFields.
 * 
 * This processor runs at compile time and generates nested Fields classes containing
 * static String constants for each field in the record, enabling type-safe field selection.
 */
@SupportedAnnotationTypes("dataset4j.annotations.GenerateFields")
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class FieldConstantProcessor extends AbstractProcessor {
    
    private Messager messager;
    private Filer filer;
    
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        this.messager = processingEnv.getMessager();
        this.filer = processingEnv.getFiler();
    }
    
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(GenerateFields.class)) {
            if (annotatedElement.getKind() != ElementKind.RECORD) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "@GenerateFields can only be applied to record classes",
                    annotatedElement
                );
                continue;
            }
            
            try {
                processRecord((TypeElement) annotatedElement);
            } catch (IOException e) {
                messager.printMessage(
                    Diagnostic.Kind.ERROR,
                    "Failed to generate fields class: " + e.getMessage(),
                    annotatedElement
                );
            }
        }
        
        return true;
    }
    
    private void processRecord(TypeElement recordElement) throws IOException {
        GenerateFields annotation = recordElement.getAnnotation(GenerateFields.class);
        String packageName = getPackageName(recordElement);
        String recordName = recordElement.getSimpleName().toString();
        String fieldsClassName = annotation.className();
        
        // Collect field information
        List<FieldInfo> fields = extractFieldInfo(recordElement, annotation);
        
        if (fields.isEmpty()) {
            messager.printMessage(
                Diagnostic.Kind.WARNING,
                "No fields found to generate constants for: " + recordName,
                recordElement
            );
            return;
        }
        
        // Generate the nested Fields class
        generateFieldsClass(packageName, recordName, fieldsClassName, fields, annotation);
        
        // Generate separate Cols class if column names are included
        if (annotation.includeColumnNames()) {
            generateColsClass(packageName, recordName, annotation.columnsClassName(), fields, annotation);
        }
        
        messager.printMessage(
            Diagnostic.Kind.NOTE,
            "Generated " + fields.size() + " field constants for " + recordName
        );
    }
    
    private List<FieldInfo> extractFieldInfo(TypeElement recordElement, GenerateFields annotation) {
        List<FieldInfo> fields = new ArrayList<>();
        
        for (Element enclosedElement : recordElement.getEnclosedElements()) {
            if (enclosedElement.getKind() == ElementKind.RECORD_COMPONENT) {
                RecordComponentElement component = (RecordComponentElement) enclosedElement;
                DataColumn dataColumn = component.getAnnotation(DataColumn.class);
                
                // Skip ignored fields unless explicitly included
                if (dataColumn != null && dataColumn.ignore() && !annotation.includeIgnored()) {
                    continue;
                }
                
                String fieldName = component.getSimpleName().toString();
                String columnName = (dataColumn != null && !dataColumn.name().isEmpty()) 
                    ? dataColumn.name() 
                    : fieldName;
                
                fields.add(new FieldInfo(fieldName, columnName, dataColumn != null));
            }
        }
        
        return fields;
    }
    
    private void generateFieldsClass(String packageName, String recordName, String fieldsClassName, 
                                   List<FieldInfo> fields, GenerateFields annotation) throws IOException {
        
        String qualifiedClassName = packageName + "." + recordName + "$" + fieldsClassName;
        JavaFileObject sourceFile = filer.createSourceFile(qualifiedClassName);
        
        try (PrintWriter writer = new PrintWriter(sourceFile.openWriter())) {
            // Package declaration
            if (!packageName.isEmpty()) {
                writer.println("package " + packageName + ";");
                writer.println();
            }
            
            // Class header with documentation
            writer.println("/**");
            writer.println(" * Generated field constants for " + recordName + " record.");
            writer.println(" * This class is automatically generated - do not modify manually.");
            writer.println(" * ");
            writer.println(" * Usage example:");
            writer.println(" * {@code");
            writer.println(" * FieldSelector.from(metadata)");
            writer.println(" *     .fields(" + recordName + "." + fieldsClassName + ".FIELD_NAME)");
            writer.println(" *     .select();");
            writer.println(" * }");
            writer.println(" */");
            writer.println("public static final class " + fieldsClassName + " {");
            writer.println();
            
            // Private constructor
            writer.println("    private " + fieldsClassName + "() {");
            writer.println("        // Utility class - prevent instantiation");
            writer.println("    }");
            writer.println();
            
            // Generate field name constants
            writer.println("    // Field name constants (Java field names)");
            for (FieldInfo field : fields) {
                String constantName = toConstantName(annotation.fieldPrefix() + field.fieldName);
                writer.println("    /** Java field name constant for '" + field.fieldName + "' */");
                writer.println("    public static final String " + constantName + " = \"" + field.fieldName + "\";");
                writer.println();
            }
            
            // Generate utility arrays
            generateUtilityArrays(writer, fields, annotation);
            
            writer.println("}");
        }
    }
    
    private void generateUtilityArrays(PrintWriter writer, List<FieldInfo> fields, GenerateFields annotation) {
        writer.println("    // Utility arrays");
        
        // Field names arrays
        writer.println("    /** Array of all Java field names */");
        writer.print("    public static final String[] ALL_FIELDS = {");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) writer.print(", ");
            String constantName = toConstantName(annotation.fieldPrefix() + fields.get(i).fieldName);
            writer.print(constantName);
        }
        writer.println("};");
        writer.println();
        
        // Annotated fields array (fields with @DataColumn)
        List<FieldInfo> annotatedFields = fields.stream()
            .filter(f -> f.hasDataColumn)
            .collect(java.util.stream.Collectors.toList());
        
        if (!annotatedFields.isEmpty()) {
            writer.println("    /** Array of field names that have @DataColumn annotations */");
            writer.print("    public static final String[] ANNOTATED_FIELDS = {");
            for (int i = 0; i < annotatedFields.size(); i++) {
                if (i > 0) writer.print(", ");
                String constantName = toConstantName(annotation.fieldPrefix() + annotatedFields.get(i).fieldName);
                writer.print(constantName);
            }
            writer.println("};");
            writer.println();
        }
    }
    
    private void generateColsClass(String packageName, String recordName, String colsClassName, 
                                  List<FieldInfo> fields, GenerateFields annotation) throws IOException {
        
        String qualifiedClassName = packageName + "." + recordName + "$" + colsClassName;
        JavaFileObject sourceFile = filer.createSourceFile(qualifiedClassName);
        
        try (PrintWriter writer = new PrintWriter(sourceFile.openWriter())) {
            // Package declaration
            if (!packageName.isEmpty()) {
                writer.println("package " + packageName + ";");
                writer.println();
            }
            
            // Class header with documentation
            writer.println("/**");
            writer.println(" * Generated column constants for " + recordName + " record.");
            writer.println(" * This class contains constants for column names from @DataColumn annotations.");
            writer.println(" * This class is automatically generated - do not modify manually.");
            writer.println(" * ");
            writer.println(" * Usage example:");
            writer.println(" * {@code");
            writer.println(" * FieldSelector.from(metadata)");
            writer.println(" *     .columns(" + recordName + "." + colsClassName + ".EMPLOYEE_ID)");
            writer.println(" *     .select();");
            writer.println(" * }");
            writer.println(" */");
            writer.println("public static final class " + colsClassName + " {");
            writer.println();
            
            // Private constructor
            writer.println("    private " + colsClassName + "() {");
            writer.println("        // Utility class - prevent instantiation");
            writer.println("    }");
            writer.println();
            
            // Generate column name constants
            writer.println("    // Column name constants (from @DataColumn annotations)");
            for (FieldInfo field : fields) {
                String columnConstantName = toConstantName(annotation.columnPrefix() + field.fieldName);
                writer.println("    /** Column name constant for field '" + field.fieldName + "' -> '" + field.columnName + "' */");
                writer.println("    public static final String " + columnConstantName + " = \"" + field.columnName + "\";");
                writer.println();
            }
            
            // Generate column utility arrays
            generateColumnUtilityArrays(writer, fields, annotation);
            
            writer.println("}");
        }
    }
    
    private void generateColumnUtilityArrays(PrintWriter writer, List<FieldInfo> fields, GenerateFields annotation) {
        writer.println("    // Utility arrays");
        
        // All column names array
        writer.println("    /** Array of all column names (from @DataColumn) */");
        writer.print("    public static final String[] ALL_COLUMNS = {");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) writer.print(", ");
            String columnConstantName = toConstantName(annotation.columnPrefix() + fields.get(i).fieldName);
            writer.print(columnConstantName);
        }
        writer.println("};");
        writer.println();
        
        // Annotated columns array (fields with @DataColumn)
        List<FieldInfo> annotatedFields = fields.stream()
            .filter(f -> f.hasDataColumn)
            .collect(java.util.stream.Collectors.toList());
        
        if (!annotatedFields.isEmpty()) {
            writer.println("    /** Array of column names for fields that have @DataColumn annotations */");
            writer.print("    public static final String[] ANNOTATED_COLUMNS = {");
            for (int i = 0; i < annotatedFields.size(); i++) {
                if (i > 0) writer.print(", ");
                String columnConstantName = toConstantName(annotation.columnPrefix() + annotatedFields.get(i).fieldName);
                writer.print(columnConstantName);
            }
            writer.println("};");
            writer.println();
        }
    }
    
    
    private String toConstantName(String fieldName) {
        // Convert camelCase to CONSTANT_CASE
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < fieldName.length(); i++) {
            char c = fieldName.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                result.append('_');
            }
            result.append(Character.toUpperCase(c));
        }
        return result.toString();
    }
    
    private String getPackageName(TypeElement element) {
        Element pkg = element.getEnclosingElement();
        while (pkg != null && pkg.getKind() != ElementKind.PACKAGE) {
            pkg = pkg.getEnclosingElement();
        }
        return pkg != null ? ((PackageElement) pkg).getQualifiedName().toString() : "";
    }
    
    /**
     * Information about a record field for constant generation.
     */
    private static class FieldInfo {
        final String fieldName;
        final String columnName;
        final boolean hasDataColumn;
        
        FieldInfo(String fieldName, String columnName, boolean hasDataColumn) {
            this.fieldName = fieldName;
            this.columnName = columnName;
            this.hasDataColumn = hasDataColumn;
        }
    }
}