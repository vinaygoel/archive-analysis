/*
 * Copyright 2013 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.archive.porky;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import java.util.regex.*;
import java.io.*;
import java.net.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.NonSpillableDataBag;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.impl.PForDeltaDocIdSet;
import com.kamikaze.docidset.impl.AndDocIdSet;
import com.kamikaze.docidset.utils.DocSetFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.Integer;

/**
 * UDF which reads bag of serialized PForDeltaDocIdSets and returns bag of sorted integer IDs that are present in all of them.
 */ 

public class FindAndIntersectionsUsingPForDeltaDocIdSetUDF extends EvalFunc<DataBag> {

  public DataBag exec(Tuple input) throws IOException {
	if (input == null || input.size() == 0) return null;
	DataBag bagOfBags = (DataBag) input.get(0);
	DocSet pForDeltaDocSet = null;
	ArrayList<DocIdSet> docs = new ArrayList<DocIdSet>();
	try {
		for (Tuple t: bagOfBags) { 
        		DataBag bag = (DataBag) t.get(0);
			pForDeltaDocSet = DocSetFactory.getPForDeltaDocSetInstance();
			for (Tuple tup : bag) {
				if (tup != null && tup.size() == 1) {
					pForDeltaDocSet.addDoc((Integer)tup.get(0));
				}
			}
			docs.add(pForDeltaDocSet);
		}
  		
		ArrayList<Integer> intersectedIds = new ArrayList<Integer>();
		AndDocIdSet andSet = new AndDocIdSet(docs);
		DocIdSetIterator iter = andSet.iterator();
		int docId = iter.nextDoc();
		while(docId != DocIdSetIterator.NO_MORE_DOCS) {
			intersectedIds.add(docId);
			docId = iter.nextDoc();
		}
		
		//return bag of intersected IDs
		DataBag resultBag = new NonSpillableDataBag(intersectedIds.size());
		for(int Id:intersectedIds) {
			Tuple newTuple = TupleFactory.getInstance().newTuple(1);
                    	newTuple.set(0, new Integer(Id));
                    	resultBag.add(newTuple);
		}
		return resultBag;	
	} catch (Exception e) {
		throw WrappedIOException.wrap("Caught exception processing input row ", e);
	}
  }
}
