/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.DD2476;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;


import org.apache.lucene.analysis.Token;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.HighFrequencyDictionary;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.lucene.search.suggest.FileDictionary;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.fst.WFSTCompletionLookup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.SolrSpellChecker;
import org.apache.solr.spelling.SpellingOptions;
import org.apache.solr.spelling.SpellingResult;
import org.apache.solr.spelling.suggest.LookupFactory;
import org.apache.solr.spelling.suggest.fst.FSTLookupFactory;
import org.apache.solr.spelling.suggest.jaspell.JaspellLookupFactory;
import org.apache.solr.spelling.suggest.tst.TSTLookupFactory;
import org.apache.solr.spelling.suggest.Suggester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// new imports
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.IndexSchema;


public class SuggesterMK2 extends SolrSpellChecker {
  private static final Logger LOG = LoggerFactory.getLogger(SuggesterMK2.class);

  /** Location of the source data - either a path to a file, or null for the
  * current IndexReader.
  */
  public static final String LOCATION = "sourceLocation";
  /** Fully-qualified class of the {@link Lookup} implementation. */
  public static final String LOOKUP_IMPL = "lookupImpl";
  /**
  * Minimum frequency of terms to consider when building the dictionary.
  */
  public static final String THRESHOLD_TOKEN_FREQUENCY = "threshold";
  /**
  * Name of the location where to persist the dictionary. If this location
  * is relative then the data will be stored under the core's dataDir. If this
  * is null the storing will be disabled.
  */
  public static final String STORE_DIR = "storeDir";

  protected String sourceLocation;
  protected File storeDir;
  protected float threshold;
  protected Dictionary dictionary;
  protected IndexReader reader;
  protected Lookup lookup;
  protected String lookupImpl;
  protected SolrCore core;

  private LookupFactory factory;

  private Map<String, List<Suggester> > delegates;
  private Set<String> suggestionFields;
  private String delimiter;
  private String endDelimiter;

  public SuggesterMK2() {
    delegates = new HashMap<String, List<Suggester> >();
    suggestionFields = new HashSet<String>();
  }

  @Override
  public String init(NamedList config, SolrCore core) {
    LOG.info("init: " + config);
    LOG.info("DELEGATE: " + config.get("delegate"));
    String name = super.init(config, core);

    // /* Remove these?
    threshold = config.get(THRESHOLD_TOKEN_FREQUENCY) == null ? 0.0f
    : (Float)config.get(THRESHOLD_TOKEN_FREQUENCY);
    sourceLocation = (String) config.get(LOCATION);
    lookupImpl = (String)config.get(LOOKUP_IMPL);
    // */

    delimiter = (String) config.get("delimiter");
    if (delimiter == null) {
      delimiter = ("\\:\"");
    }

    endDelimiter = (String) config.get("endDelimiter");
    if(endDelimiter == null){
      endDelimiter = "\"";
    }

    // Create configuration with the parts common to all delegates:
    NamedList delegateBaseConfig = config.clone();
    delegateBaseConfig.removeAll("classname");
    delegateBaseConfig.add("classname", "org.apache.solr.spelling.suggest.Suggester");

    Collection<NamedList<String> > allFields = delegateBaseConfig.removeAll("delegate");
    if (allFields == null) {
      // No fields marked in solrconfig.xml
      // Default to all fields in schema
      allFields = new ArrayList<NamedList<String> >();
      for (String field : core.getLatestSchema().getFields().keySet()) {
        NamedList<String> delegatePair = new NamedList<String>();
        delegatePair.add("targetField", field);
        delegatePair.add("sourceField", field);
        allFields.add(delegatePair);
      }
    }
    for (NamedList<String> fieldDelegation : allFields) {
      String targetField = fieldDelegation.get("targetField");
      LOG.info("Creating delegates for field: " + targetField);

      delegates.put(targetField, new ArrayList<Suggester>());

      for (String sourceField : fieldDelegation.getAll("sourceField")) {
        LOG.info("Creating delegate source: " + sourceField);
        // Create configuration for this delegate suggester
        NamedList delegateConfig = delegateBaseConfig.clone();
        delegateConfig.add("field", sourceField);

        Suggester newSuggester = new Suggester();
        newSuggester.init(delegateConfig, core);
        delegates.get(targetField).add(newSuggester);
        suggestionFields.add(targetField);
      }
    }

    return name;
  }

  @Override
  public void build(SolrCore core, SolrIndexSearcher searcher) throws IOException {
    LOG.info("build()");
    for (Map.Entry<String, List<Suggester> > entry : delegates.entrySet()) {
      for (Suggester target : entry.getValue()) {
        target.build(core, searcher);
      }
    }
  }

  @Override
  public void reload(SolrCore core, SolrIndexSearcher searcher) throws IOException {
    LOG.info("reload()");
    for (Map.Entry<String, List<Suggester> > entry : delegates.entrySet()) {
      for (Suggester target : entry.getValue()) {
        target.reload(core, searcher);
      }
    }
  }

  static SpellingResult EMPTY_RESULT = new SpellingResult();

  private Query getField(Collection<Token> tokens){
    Query query = new Query();
    // Iterator over each token
    for(Token token : tokens){

      String scratch = getScratch(token);
      // Split on field query completion syntax delimiter :"
      String[] fieldValues = scratch.split(delimiter);
      // If such syntax is used, identify the field
      if (fieldValues.length == 2) {

        query.field = fieldValues[0];
        query.value = fieldValues[1];

        if(query.value.indexOf("\"") != -1){
          query.field = null;
          query.value = null;
        }
      } else {
        if(scratch.indexOf("\"") != -1){
          query.field = null;
          query.value = null;
        } else {
          if(query.value == null){
            query.value = scratch;
          } else {
            query.value += " " + scratch;
          }
        }
      }
    }
    return query;
  }

  private String getScratch(Token token){
    CharsRef scratch = new CharsRef();
    scratch.chars = token.buffer();
    scratch.offset = 0;
    scratch.length = token.length();
    return scratch.toString();
  }



  class Query {
    String field;
    String value;
  }

  @Override
  public SpellingResult getSuggestions(SpellingOptions options) throws IOException {
    LOG.debug("getSuggestions (MK2): " + options.tokens);

      SpellingResult res = new SpellingResult();

      Query query = getField(options.tokens);
      String targetField = query.field;
      String scratch = query.value;
      LOG.info("Query: " + targetField + ", " + scratch);
      Token t = null;
      for(Token token : options.tokens){
        t = token;
      }

      // LOG.debug("CharsRef: " + scratch.toString());
      boolean onlyMorePopular = (options.suggestMode == SuggestMode.SUGGEST_MORE_POPULAR);
      // List<LookupResult> suggestions = lookup.lookup(scratch, onlyMorePopular, options.count); // scratch = query (key).
      List<LookupResult> suggestions = new ArrayList<LookupResult>();

      // Solr splits on the following characters automatically
      // : ; , . + ( ) { } '
      // so they can't be used as field delimiters
      // - _ works

      LOG.info("Targetfield: " + targetField);

      // or field_value.length > 1 ?
      if (targetField != null) {
        // Autocomplete field value:
        String targetValue = scratch;
        if(targetValue.startsWith(targetField)){
          targetValue = targetValue.split(delimiter)[1];
        }

        LOG.info("Delegate to field: " + targetField);
        if (!delegates.containsKey(targetField)) {
          LOG.info("No such field: " + targetField);
          return res;
        }

        // Construct new options for delegate:
        ArrayList<Token> tokens = new ArrayList<Token>();
        tokens.add(new Token(targetValue, 0, targetValue.length()));
        SpellingOptions delegateOptions = new SpellingOptions(tokens, options.count);
        LOG.info("new tokens: " + delegateOptions.tokens);

        // Get results from delegate
        suggestions.addAll(getSuggestions(delegates.get(targetField), delegateOptions, targetField + delimiter));
      } else {
      // If it didn't match a field
        // Autocomplete field name:
        for (String field : suggestionFields) {
          if(field.startsWith(scratch.toString())) {
            // Sort field completions first
			  long weight = Long.MAX_VALUE;
		      suggestions.add(new LookupResult(field + delimiter, weight));
          }
        }

        // Autocomplete field contents
        // Copypasted code: TODO: merge with above
        for (Map.Entry<String, List<Suggester> > delegateEntry : delegates.entrySet()) {
          // Get results from delegate
          suggestions.addAll(getSuggestions(delegateEntry.getValue(), options, ""));
        }
      }


      
      
      
      if (options.suggestMode == SuggestMode.SUGGEST_MORE_POPULAR) {
		// Sort lexically to merge duplicates
		Collections.sort(suggestions);
		
		List<LookupResult> newSuggestions = new ArrayList<LookupResult>();
		// Merge duplicate results and add their weights
		// together.
		// TODO: is there a better way to do this?
		// prevResult is the last result with the same
		// key as the current
		LookupResult prevResult = null;
		long cumValue = 0;
		for (LookupResult lr : suggestions) {
		  if (prevResult != null && lr.key.equals(prevResult.key)) {
		    cumValue += lr.value;
		  } else {
		    if (prevResult != null) {
			  cumValue += prevResult.value;
			  newSuggestions.add(new LookupResult(prevResult.key, cumValue));
			  cumValue = 0;
			}
		    prevResult = lr;
		  }
		}
		if (prevResult != null) {
		  cumValue += prevResult.value;
		  newSuggestions.add(new LookupResult(prevResult.key, cumValue));
		}
		suggestions = newSuggestions;
		
		// Sort by weight
		Collections.sort(suggestions,
		  new Comparator<LookupResult>() {
		    public int compare(LookupResult a, LookupResult b) {
			  // value (weight) is long
			  
			  // This doesn't work unless difference fits in int:
		      // XXX: return (int)(b.value - a.value)
			  
			  // Do it the stupid way:
			  if (a.value < b.value) {
				return 1;
			  } else if (a.value > b.value) {
				return -1;
			  } else {
				return 0;
			  }
		    }
		  }
	    );
	  } else {
		    LOG.info("Sorting: ");
        Collections.sort(suggestions);
      }
      for (LookupResult lr : suggestions) {
		LOG.info("Result: " + lr.key + " (weight: " + lr.value + ")");
        res.add(t, lr.key.toString(), (int)lr.value);
      }
    return res;
  }

  public List<LookupResult> getSuggestions(List<Suggester> suggesters, SpellingOptions options, String prefix) throws IOException{
    // Get results from delegate
    List<LookupResult> suggestions = new ArrayList<LookupResult>();

    for (Suggester target : suggesters) {
      SpellingResult delegateResults = target.getSuggestions(options);
      for (Map.Entry<Token, LinkedHashMap<String, Integer> > entry : delegateResults.getSuggestions().entrySet()) {
        for (Map.Entry<String, Integer> key_weight : entry.getValue().entrySet()) {
          String key = prefix + key_weight.getKey();
          int weight = key_weight.getValue();
          suggestions.add(new LookupResult(key, weight));
        }
      }
    }
    return suggestions;
  }


}
