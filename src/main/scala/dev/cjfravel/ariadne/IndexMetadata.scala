package dev.cjfravel.ariadne

import com.google.gson.annotations.SerializedName
import java.util
import com.google.gson.Gson

case class IndexMetadata(
    @SerializedName("format") var format: String,
    @SerializedName("schema") var schema: String,
    @SerializedName("indexes") var indexes: util.List[String],
    @SerializedName("computed_index") var computed_indexes: util.Map[
      String,
      String
    ]
)

object IndexMetadata {
  def apply(jsonString: String): IndexMetadata = {
    val indexMetadata = new Gson().fromJson(jsonString, classOf[IndexMetadata])
    // v1 -> v2
    if (indexMetadata.computed_indexes == null) {
      indexMetadata.computed_indexes =
        new util.HashMap[String, String]()
    }
    indexMetadata
  }
}
