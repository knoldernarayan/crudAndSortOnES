package com.narayan

import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.client.Client._
import org.elasticsearch.node.Node._
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import scala.io.Source
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.action.search.SearchResponse

/**
 * @author narayan kumar
 *
 */
trait ESOperation {

  /**
   * This is getClient method which returns java API client
   *
   * @return
   */
  def getClient(): Client = {
    val node = nodeBuilder().local(true).node()
    val client = node.client()
    client
  }

  val mappingBuilder = (jsonBuilder()
    .startObject()
    .startObject("twitter")
    .startObject("_timestamp")
    .field("enabled", true)
    .field("store", true)
    .field("path", "post_date")
    .endObject()
    .endObject()
    .endObject())

  /**
   * This is addMappingToIndex method which provides settings and mappings to index and  create it
   *
   * @param indexName
   * @param client
   * @return
   */
  def addMappingToIndex(indexName: String, client: Client): CreateIndexResponse = {

    val settingsStr = ImmutableSettings.settingsBuilder().
      put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build()
    client.admin().indices().prepareCreate(indexName)
      .setSettings(settingsStr)
      .addMapping(indexName, mappingBuilder).execute()
      .actionGet()

  }

  /**
   * This is insertBulkDocument method which takes each document from file and insert into index
   *
   * @param client
   * @return
   */
  def insertBulkDocument(client: Client): BulkResponse = {
    val bulkJson = Source.fromFile("src/main/resources/bulk.json").getLines().toList
    val bulkRequest = client.prepareBulk()
    for (i <- 0 until bulkJson.size) {
      bulkRequest.add(client.prepareIndex("twitter", "tweet", (i + 1).toString).setSource(bulkJson(i)))
    }
    bulkRequest.execute().actionGet()
  }

  /**
   * This is update index method which updates particular document by add one more field
   *
   * @param client
   * @param indexName
   * @param typeName
   * @param id
   * @return
   */
  def updateIndex(client: Client, indexName: String, typeName: String, id: String): UpdateResponse = {

    val updateRequest = new UpdateRequest(indexName, typeName, id)
      .doc(jsonBuilder()
        .startObject()
        .field("gender", "male")
        .endObject())
    client.update(updateRequest).get()
  }

  /**
   * This is sortByTimeStamp method provides sorted document on the basis of time stamp
   *
   * @param client
   * @param indexName
   * @return
   */
  def sortByTimeStamp(client: Client, indexName: String): SearchResponse = {
    val filter = andFilter(rangeFilter("post_date").from("2015-05-13") to ("2015-05-19"))
    val sortedSearchResponse = client.prepareSearch().setIndices(indexName)
      .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter))
      .setSize(2).addSort("post_date", SortOrder.DESC).execute().actionGet()
    sortedSearchResponse
  }

  /**
   * This is deleteDocumentById method which removes particular document from index
   *
   * @param client
   * @param indexName
   * @param typeName
   * @param id
   * @return
   */
  def deleteDocumentById(client: Client, indexName: String, typeName: String, id: String): DeleteResponse = {

    val delResponse = client.prepareDelete("twitter", "tweet", "1")
      .execute()
      .actionGet()
    delResponse
  }

  /**
   * This is deleteIndex method which takes client and index as parameter and delete index from node
   *
   * @param client
   * @param indexName
   * @return
   */
  def deleteIndex(client: Client, indexName: String): Boolean = {

    val deleteIndexRequest = new DeleteIndexRequest(indexName)
    val deleteIndexResponse = client.admin().indices().delete(deleteIndexRequest).actionGet()
    deleteIndexResponse.isAcknowledged()
  }

}