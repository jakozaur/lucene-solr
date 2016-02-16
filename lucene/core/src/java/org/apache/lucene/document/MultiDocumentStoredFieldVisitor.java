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

package org.apache.lucene.document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

/** A {@link org.apache.lucene.index.StoredFieldVisitor} that creates a list of {@link
 *  Document} containing all stored fields, or only specific
 *  requested fields provided to {@link #MultiDocumentStoredFieldVisitor(Set)}.
 *  <p>
 *  This is used by {@link org.apache.lucene.index.IndexReader#documents(int[])} to load a
 *  document.
 *
 * @lucene.experimental */
public class MultiDocumentStoredFieldVisitor extends StoredFieldVisitor {
  private final Set<String> fieldsToAdd;
  private Document currentDocument = new Document();
  private final ArrayList<Document> documents = new ArrayList<Document>();

  /**
   * Load only fields named in the provided <code>Set&lt;String&gt;</code>.
   * @param fieldsToAdd Set of fields to load, or <code>null</code> (all fields).
   */
  public MultiDocumentStoredFieldVisitor(Set<String> fieldsToAdd) {
    this.fieldsToAdd = fieldsToAdd;
  }

  /** Load all stored fields. */
  public MultiDocumentStoredFieldVisitor() {
    this.fieldsToAdd = null;
  }

  public void newDocument() {
    documents.add(currentDocument);
    currentDocument = new Document();
  }

  @Override
  public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
    currentDocument.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
    final FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(fieldInfo.hasVectors());
    ft.setOmitNorms(fieldInfo.omitsNorms());
    ft.setIndexOptions(fieldInfo.getIndexOptions());
    currentDocument.add(new StoredField(fieldInfo.name, new String(value, StandardCharsets.UTF_8), ft));
  }

  @Override
  public void intField(FieldInfo fieldInfo, int value) {
    currentDocument.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public void longField(FieldInfo fieldInfo, long value) {
    currentDocument.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public void floatField(FieldInfo fieldInfo, float value) {
    currentDocument.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public void doubleField(FieldInfo fieldInfo, double value) {
    currentDocument.add(new StoredField(fieldInfo.name, value));
  }

  @Override
  public Status needsField(FieldInfo fieldInfo) throws IOException {
    return fieldsToAdd == null || fieldsToAdd.contains(fieldInfo.name) ? Status.YES : Status.NO;
  }

  /**
   * Retrieve the visited documents.
   * @return List of {@link Document} populated with stored fields. Note that only
   *         the stored information in the field instances is valid,
   *         data such as indexing options, term vector options,
   *         etc is not set.
   */
  public List<Document> getDocuments() {
    return documents;
  }
}