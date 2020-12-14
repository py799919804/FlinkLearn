package elasticsearch;//package com.ultrapower.fsms.common.elasticsearch;

import elasticsearch.util.ESConstans;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @Title: 资源数据ES处理
 * @description:
 *
 * @Company: ultrapower.com
 * @author lnj2050
 * @create time：2016年5月17日 下午4:04:01
 * @version 1.0
 */
public class ESResourceDataHandler extends ESDataHandler {

    // 日志
    private static Logger log = LoggerFactory.getLogger(ESResourceDataHandler.class);

    /**
     * 批量创建索引
     * @param resMapList
     */
    public void createResourceIndexBulk(List<Map> resMapList) {
        if (resMapList == null || resMapList.size() <= 0) {
            log.error("createResourceIndexBulk resMapList is null");
            return;
        }
        // 资源索引:{appId}_resource     类型：sigmamc
        String curIndexType = ESConstans.INDEX_TYPE;
        int size = resMapList.size();
        List<IndexRequest> requests = new ArrayList<IndexRequest>();
        for (int i = 0; i < size; i++) {
            Map map = resMapList.get(i);
            String resId = "" + map.get("RESID");
            if (StringUtils.isEmpty(resId) || resId.equalsIgnoreCase("null")) {
                continue;
            }
            String curIndexName = ESConstans.getResourceIndex((String)map.get(DefaultConstants.PROPERTYTITLE_APPID));
            map.remove(DefaultConstants.PROPERTYTITLE_APPID);

            // 插入sortIP
			String ip = (String) map.get("IP");
			if (ip != null && !ip.equalsIgnoreCase("") && !ip.equalsIgnoreCase("null")) {
				if (ip.indexOf(",") > 0) { //多个ip，逗号分隔
					String[] ipary = ip.split(",");
					String firstIp = ipary[0];
					if (firstIp != null) {
						String[] firstIpAry = firstIp.split("\\.");
						if (firstIpAry != null && firstIpAry.length == 4) {
							map.put("SORTIP", firstIp);
						}
					}
				} else {
					map.put("SORTIP", ip);
				}
			}
			// 更新对象也调的整个方法，如果不写的话，保存到es中，updatetime就没了
			map.put("UPDATETIME", System.currentTimeMillis());
            IndexRequest request =
                    getTransportClient().prepareIndex(curIndexName, curIndexType, resId).setSource(map).request();
            requests.add(request);
        }

        BulkRequestBuilder bulkRequest = getTransportClient().prepareBulk();

        for (IndexRequest request : requests) {
            bulkRequest.add(request);
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            log.error("createResourceIndexBulk has failures, " + bulkResponse.buildFailureMessage());
        } else {
            log.info("createResourceIndexBulk size=" + size + " sucessfully!");
        }
    }

	/**
	 * 获取查询结果
	 *
	 * @param queryBuilder
	 * @param queryStr
	 * @param sortBuilder
	 * @param tmpFirstIdx
	 * @param pageSize
	 * @return
	 */
	public Map<String, Object> getSearchHits(QueryBuilder queryBuilder, String queryStr, SortBuilder sortBuilder,
                                             int tmpFirstIdx, int pageSize, String appid) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("resIds", new ArrayList<Long>());
		map.put("total", 0);
		// SearchHits searchHits = null;
		SearchRequestBuilder srb = null;
		String resIndex = ESConstans.getResourceIndex(appid);
		try {
			srb = getTransportClient().prepareSearch(resIndex).setTypes(ESConstans.INDEX_TYPE);
			if (queryBuilder != null && queryStr != null && !queryStr.equals("")) {
				BoolQueryBuilder strQueryBuilder = getBoolQueryBuilder(queryStr);
				BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(strQueryBuilder).must(queryBuilder);
				srb = srb.setQuery(boolQueryBuilder);
			} else {
				if (queryBuilder != null) {
					srb = srb.setQuery(queryBuilder);
				}

				if (queryStr != null && !queryStr.equals("")) {
					srb = srb.setQuery(getBoolQueryBuilder(queryStr));
				}
			}

			if (tmpFirstIdx >= 10000) {
				SearchHits searchHits = null;
				int curpage = tmpFirstIdx / pageSize;
				if (sortBuilder != null) {
					srb = srb.addSort(sortBuilder);
				}
				SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(pageSize).execute().actionGet();
				for (int i = 0; i < curpage; i++) {
					scrollResp = getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
							.setScroll(new TimeValue(600000)).execute().actionGet();
					if (i == curpage - 1) {
						if (scrollResp != null) {
							searchHits = scrollResp.getHits();
						}
					}
				}

				if (searchHits != null) {
					List<String> residList = new ArrayList<String>();
					for (SearchHit hit : searchHits.getHits()) {
						residList.add(hit.getId());
					}
					map.put("resIds", residList);
					map.put("total", searchHits.getTotalHits());
				}
			} else if (pageSize >= 10000) {
				SearchResponse searchResponse = srb.setScroll(new TimeValue(60000)).setSize(10000).execute()
						.actionGet();
				if (searchResponse != null) {
					SearchHits searchHit = searchResponse.getHits();
					long cursize = searchHit.getHits().length;
					if (cursize == 0) {
						return map;
					}
					List<String> residList = new ArrayList<String>();
					for (SearchHit hit : searchHit) {
						residList.add(hit.getId());
					}

					while (true) {
						searchResponse = getTransportClient().prepareSearchScroll(searchResponse.getScrollId())
								.setScroll(new TimeValue(600000)).execute().actionGet();

						searchHit = searchResponse.getHits();
						cursize = searchHit.getHits().length;

						// 再次查询不到数据时跳出循环
						if (cursize == 0) {
							break;
						}
						for (SearchHit hit : searchHit) {
							residList.add(hit.getId());
						}
					}
					map.put("resIds", residList);
					map.put("total", cursize);
				}
			} else {
				if (sortBuilder != null) {
					srb = srb.addSort(sortBuilder);
				}
				SearchResponse searchResponse = srb.setFrom(tmpFirstIdx).setSize(pageSize).execute().actionGet();
				if (searchResponse != null) {
					SearchHits searchHits = searchResponse.getHits();
					if (searchHits != null) {
						List<String> residList = new ArrayList<String>();
						for (SearchHit hit : searchHits.getHits()) {
							residList.add(hit.getId());
						}
						map.put("resIds", residList);
						map.put("total", searchHits.getTotalHits());
					}
				}
			}

			//log.info(">>>>>>>> search condition >>>>>> " + srb.toString().replaceAll("\n", ""));
		} catch (Exception ex) {
			log.error("getSearchHits error!!!  condition >>>>>> " + srb.toString().replaceAll("\n", ""), ex);
		}
		return map;
	}

    /**
     * 获取多个字段的统计结果
     * @param filterBuilder
     * @param queryStr
     * @param fields
     * @return
     */
    public Map<String, Map<String, Long>> getAggregations(QueryBuilder filterBuilder, String queryStr, String[] fields, String appid) {
        if (fields == null) {
            return null;
        }
        // 获取统计信息
        Map<String, Map<String, Long>> fieldsMap = new HashMap<String, Map<String, Long>>();
        String resIndex = ESConstans.getResourceIndex(appid);
        try {
            SearchRequestBuilder srb = getTransportClient().prepareSearch(resIndex).setTypes(ESConstans.INDEX_TYPE);
            // 查询条件
            if (filterBuilder != null && queryStr != null && !queryStr.equals("")) {
                BoolQueryBuilder strQueryBuilder = getBoolQueryBuilder(queryStr);
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(strQueryBuilder).filter(filterBuilder);
                srb = srb.setQuery(boolQueryBuilder);
            } else {
                if (filterBuilder != null) {
                    srb = srb.setQuery(filterBuilder);
                }
                if (queryStr != null && !queryStr.equals("")) {
                    srb = srb.setQuery(getBoolQueryBuilder(queryStr));
                }
            }

            // 设置统计字段
            for (String field : fields) {
                if (field == null || field.equals("")) {
                    continue;
                }
                srb.addAggregation(AggregationBuilders.terms(field).field(field).size(5000));
            }

            log.debug(">>>>>>> conditon >>>>>>>>>" + srb.toString().replaceAll("\n", ""));

            SearchResponse sr = srb.execute().actionGet();

            for (String field : fields) {
                if (field == null || field.equals("")) {
                    continue;
                }
                Map<String, Long> fieldMap = new HashMap<String, Long>();
                Terms terms = sr.getAggregations().get(field);
                for (Bucket b : terms.getBuckets()) {
                    fieldMap.put(String.valueOf(b.getKey()), b.getDocCount());
                }
                fieldsMap.put(field, fieldMap);
            }
        } catch (Exception e) {
            log.error("getAggregations error", e);
        }

        return fieldsMap;
    }

    /**
     * 更新资源属性信息
     * @param resids
     * @param appId
     * @param objMap
     */
	public void updateResourceAttribute(long[] resids, String appId, Map<String, Object> objMap) {
		String resIndex = ESConstans.getResourceIndex(appId);
		BulkRequestBuilder bulkRequest = getTransportClient().prepareBulk();
		// 遍历每个资源
		for (long resid : resids) {
			if (resid == 0) {
				continue;
			}
			UpdateRequest updateRequest = new UpdateRequest();
			updateRequest.index(resIndex).type(ESConstans.INDEX_TYPE).id(String.valueOf(resid));
			try {
				XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject();
				// 遍历每个属性
				Iterator it = objMap.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<String, Object> me = (Map.Entry<String, Object>) it.next();
					// updatetime不更新
					if (me.getKey() != null && me.getKey().equalsIgnoreCase("UPDATETIME")) {
						continue;
					}
					contentBuilder.field(me.getKey(), me.getValue());
				}
				contentBuilder.endObject();
				updateRequest.doc(contentBuilder);
			} catch (IOException e) {
				log.error("updateRequest error, resid=" + resid, e);
			}
			bulkRequest.add(updateRequest);
		}
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			log.error("updateResourceAttribute has failures, " + bulkResponse.buildFailureMessage());
		} else {
			log.info("updateResourceAttribute size=" + resids.length + " sucessfully!");
		}
	}

	/**
	 * 根据查询条件获取资源id
	 * @param queryBuilder
	 * @param appId
	 * @return
	 */
	public List<String> getResIdsByQueryBuilder(QueryBuilder queryBuilder, String appId) {
		List<String> results = new ArrayList<String>();
		String indexName = ESConstans.getResourceIndex(appId);
		try {
			SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(ESConstans.INDEX_TYPE);
			if (queryBuilder != null) {
				srb = srb.setQuery(queryBuilder);
			}

			SearchResponse searchResponse = srb.setScroll(new TimeValue(60000)).setSize(500).execute().actionGet();
			SearchHits searchHits = searchResponse.getHits();
			SearchHit[] searchHitAry = searchHits.getHits();
			long cursize = searchHitAry.length;
			if (cursize == 0) {
				return results;
			}

			if (searchHitAry != null) {
				for (SearchHit hit : searchHitAry) {
					if (hit == null) {
						log.warn("hit is null");
						continue;
					}
					String hitId = hit.getId();
					results.add(hitId);
				}
			}

			while (true) {
				searchResponse = getTransportClient().prepareSearchScroll(searchResponse.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();

				searchHits = searchResponse.getHits();
				searchHitAry = searchHits.getHits();
				cursize = searchHitAry.length;

				// 再次查询不到数据时跳出循环
				if (cursize == 0) {
					break;
				}
				for (SearchHit hit : searchHitAry) {
					if (hit == null) {
						log.warn("hit is null");
						continue;
					}
					String hitId = hit.getId();
					results.add(hitId);
				}
			}
		} catch (Exception ex) {
			log.error("getResIdsByQueryBuilder error!! queryBuilder=" + queryBuilder, ex);
		}
		return results;
	}

	/**
	 * 多字段分组
	 * @param filterBuilder
	 * @param queryStr
	 * @param field1
	 * @param field2
	 * @param appid
	 * @return
	 */
	public Map<String, Map<String,Long>> getSubAggregations(QueryBuilder filterBuilder, String queryStr, String field1,
                                                            String field2, String appid) {
		if (field1 == null) {
			return null;
		}
		// 获取统计信息
		Map<String, Map<String,Long>> map = new HashMap<String, Map<String,Long>>();
		String resIndex = ESConstans.getResourceIndex(appid);
		try {
			SearchRequestBuilder srb = getTransportClient().prepareSearch(resIndex).setTypes(ESConstans.INDEX_TYPE);
			// 查询条件
			if (filterBuilder != null && queryStr != null && !queryStr.equals("")) {
				BoolQueryBuilder strQueryBuilder = getBoolQueryBuilder(queryStr);
				BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(strQueryBuilder)
						.filter(filterBuilder);
				srb = srb.setQuery(boolQueryBuilder);
			} else {
				if (filterBuilder != null) {
					srb = srb.setQuery(filterBuilder);
				}
				if (queryStr != null && !queryStr.equals("")) {
					srb = srb.setQuery(getBoolQueryBuilder(queryStr));
				}
			}

			// 设置统计字段
			TermsAggregationBuilder fieldTermsBuilder = AggregationBuilders.terms(field1).field(field1).size(5000);
			TermsAggregationBuilder fieldTermsBuilder2 = AggregationBuilders.terms(field2).field(field2).size(5000);
			fieldTermsBuilder.subAggregation(fieldTermsBuilder2);
			srb.addAggregation(fieldTermsBuilder);

			log.debug(">>>>>>> conditon >>>>>>>>>" + srb.toString().replaceAll("\n", ""));

			SearchResponse sr = srb.execute().actionGet();

			Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
			StringTerms gradeTerms = (StringTerms) aggMap.get(field1);
			Iterator<StringTerms.Bucket> gradeBucketIt = gradeTerms.getBuckets().iterator();
			while (gradeBucketIt.hasNext()) {
				Map<String,Long> subMap = new HashMap<String,Long>();
				Bucket gradeBucket = gradeBucketIt.next();
				String gkey = (String) gradeBucket.getKey();
				if (gkey == null || gkey.equals("") || gkey.equalsIgnoreCase("null")) {
					continue;
				}

				StringTerms classTerms = (StringTerms) gradeBucket.getAggregations().asMap().get(field2);
				Iterator<StringTerms.Bucket> classBucketIt = classTerms.getBuckets().iterator();
				while (classBucketIt.hasNext()) {
					Bucket classBucket = classBucketIt.next();
					String ckey = (String) classBucket.getKey();
					long docCount = classBucket.getDocCount() ;
					if (ckey == null || ckey.equals("") || ckey.equalsIgnoreCase("null")) {
						continue;
					}
					subMap.put(ckey, docCount);
				}
				map.put(gkey, subMap);
			}

		} catch (Exception e) {
			log.error("getAggregations error", e);
		}

		return map;
	}

	/**
	 * 获取所有的资源id和searchcode
	 * @param queryBuilder
	 * @param queryStr
	 * @param sortBuilder
	 * @param appId
	 * @return
	 */
	public Map<String, String> getAllResMap(QueryBuilder queryBuilder, String queryStr, SortBuilder sortBuilder,
                                            String appId) {
		Map<String, String> map = new HashMap<>();
		SearchRequestBuilder srb = null;
		String resIndex = ESConstans.getResourceIndex(appId);
		try {
			srb = getTransportClient().prepareSearch(resIndex).setTypes(ESConstans.INDEX_TYPE);
			if (queryBuilder != null && queryStr != null && !queryStr.equals("")) {
				BoolQueryBuilder strQueryBuilder = getBoolQueryBuilder(queryStr);
				BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(strQueryBuilder).must(queryBuilder);
				srb = srb.setQuery(boolQueryBuilder);
			} else {
				if (queryBuilder != null) {
					srb = srb.setQuery(queryBuilder);
				}

				if (queryStr != null && !queryStr.equals("")) {
					srb = srb.setQuery(getBoolQueryBuilder(queryStr));
				}
			}

			if (sortBuilder != null) {
				srb = srb.addSort(sortBuilder);
			}

			long t1 = System.currentTimeMillis();
			SearchResponse scrollResp = srb.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
					.setScroll(new TimeValue(60000)).setSize(5000).get();

			do {
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					Map<String, Object> rowMap = hit.getSourceAsMap();
					map.put(hit.getId(), (String) rowMap.get("SEARCHCODE"));
				}

				scrollResp = getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
						.setScroll(new TimeValue(60000)).execute().actionGet();
			} while (scrollResp.getHits().getHits().length != 0);
			long t2 = System.currentTimeMillis();
			log.info("query all resource, times=" + (t2 - t1) / 1000 + "s");
		} catch (Exception e) {
			log.error("getAggregations error", e);
		}

		return map;
	}
}

