package com.narayan

object CrudOnElasticSearch extends ESOperation with App {

  val client = getClient()
  val mappingResponse = addMappingToIndex("twitter", client)
  println("@@@@@ mapping response is " + mappingResponse.isAcknowledged())
  val bulkInsertResponse = insertBulkDocument(client)
  println("@@@@@@ number of documents inserted by bulk request  is " + bulkInsertResponse.getItems.length)
  Thread.sleep(800)
  val sortedResult = sortByTimeStamp(client, "twitter")
  println("@@@@@@ sorted by time stamp and returns number filtered document " + sortedResult.getHits.getHits.length)
  val updateResponse = updateIndex(client, "twitter", "tweet", "1")
  println("@@@@@@@@ update response document version is " + updateResponse.getVersion)
  val deleteDocument = deleteDocumentById(client, "twitter", "tweet", "1")
  println("@@@@@ deleted document by id is " + deleteDocument.isFound())
  val deleteIndexResponse = deleteIndex(client, "twitter")
  println("@@@@@ delete index response " + deleteIndexResponse)
}