package dev.cjfravel.ariadne

import com.google.gson.annotations.SerializedName
import java.util
import com.google.gson.Gson

/** Represents a mapping for exploded field index configuration.
  *
  * This case class defines how to extract and index fields from array elements.
  * It maps array elements to a flat structure that can be used for efficient joins
  * by exploding the array and indexing specific fields within each element.
  *
  * @param array_column The name of the array column in the schema
  * @param field_path The field path to extract from each array element (e.g., "id", "profile.user_id")
  * @param as_column The alias name to use for this extracted field in joins
  *
  * @example
  * {{{
  * // For a schema with users: Array[Struct{id: Int, name: String}]
  * val mapping = ExplodedFieldMapping("users", "id", "user_id")
  * // This allows joining on "user_id" which represents users[].id
  * }}}
  */
case class ExplodedFieldMapping(
    var array_column: String,
    var field_path: String,
    var as_column: String
)

/** Metadata container for Ariadne index configuration and state.
  *
  * This case class stores all metadata required to manage an Ariadne index,
  * including schema information, indexed columns, and format details.
  * It supports multiple versions for backward compatibility.
  *
  * @param format The file format of the indexed data (e.g., "parquet", "csv")
  * @param schema The JSON representation of the DataFrame schema
  * @param indexes List of regular column names that are indexed
  * @param computed_indexes Map of computed index aliases to their SQL expressions
  * @param exploded_field_indexes List of exploded field mappings for nested data structures
  *
  * @note The field names use underscore notation to match JSON serialization format
  */
case class IndexMetadata(
    var format: String,
    var schema: String,
    var indexes: util.List[String],
    var computed_indexes: util.Map[
      String,
      String
    ],
    var exploded_field_indexes: util.List[ExplodedFieldMapping]
)

/** Factory object for creating IndexMetadata instances from JSON.
  *
  * Provides deserialization capabilities with automatic version migration
  * to ensure backward compatibility across different metadata formats.
  */
object IndexMetadata {
  
  /** Creates an IndexMetadata instance from a JSON string.
    *
    * This method deserializes JSON metadata and automatically handles version
    * migration to ensure compatibility with older index formats:
    * - v1 → v2: Adds computed_indexes field if missing
    * - v2 → v3: Adds exploded_field_indexes field if missing
    *
    * @param jsonString The JSON representation of the metadata
    * @return A fully initialized IndexMetadata instance
    *
    * @example
    * {{{
    * val jsonString = """{"format":"parquet","schema":"...","indexes":["id"]}"""
    * val metadata = IndexMetadata(jsonString)
    * // Returns metadata with all fields properly initialized
    * }}}
    */
  def apply(jsonString: String): IndexMetadata = {
    val indexMetadata = new Gson().fromJson(jsonString, classOf[IndexMetadata])
    // v1 -> v2
    if (indexMetadata.computed_indexes == null) {
      indexMetadata.computed_indexes =
        new util.HashMap[String, String]()
    }
    // v2 -> v3
    if (indexMetadata.exploded_field_indexes == null) {
      indexMetadata.exploded_field_indexes = new util.ArrayList[ExplodedFieldMapping]()
    }
    indexMetadata
  }
}
