/*
 * Copyright 2015 fdefalco.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ohdsi.webapi.service;

import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import org.ahocorasick.trie.Emit;
import org.xml.sax.InputSource;
import org.ahocorasick.trie.Trie;
import org.ohdsi.webapi.vocabulary.Concept;
import org.ohdsi.webapi.vocabulary.ConceptSearch;
import org.springframework.beans.factory.annotation.Autowired;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

@Path("/spl/")
@Component
public class SPLService {

  @Autowired
  private VocabularyService vocabularyService;

  @Path("{setId}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public String getSPL() {
    String url = "http://dailymed.nlm.nih.gov/dailymed/services/v2/spls/1efe378e-fee1-4ae9-8ea5-0fe2265fe2d8.xml";

    // would want to cache this trie
    Trie trie = new Trie().caseInsensitive().onlyWholeWords().removeOverlaps();
    ConceptSearch search = new ConceptSearch();
    search.vocabularyId = new String[]{"SNOMED"};
    search.conceptClassId = new String[]{"Clinical Finding"};
    search.query = "";
    Collection<Concept> concepts = vocabularyService.executeSearch("VOCAB", search);
    HashMap<String, Concept> conceptLookup = new HashMap<>();

    for (Concept c : concepts) {
      trie.addKeyword(c.conceptName.toLowerCase());
      conceptLookup.put(c.conceptName.toLowerCase(), c);
    }

    URL target;
    try {
      target = new URL(url);
    } catch (MalformedURLException ex) {
      throw new RuntimeException("bad url " + url);
    }

    InputSource source;
    String result;
    try {
      source = new InputSource(target.openStream());
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      Document doc = db.parse(source);

      XPathFactory xPathfactory = XPathFactory.newInstance();
      XPath xpath = xPathfactory.newXPath();
      XPathExpression expr = xpath.compile("//paragraph");
      NodeList nl = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

      for (int i = 0; i < nl.getLength(); i++) {
        String textToAnalyze = nl.item(i).getTextContent();
        Collection<Emit> emits = trie.parseText(textToAnalyze);

        ArrayList<String> uniqueEmits = new ArrayList<>();
        for (Emit emit : emits) {
          if (!uniqueEmits.contains(emit.getKeyword())) {
            uniqueEmits.add(emit.getKeyword());
          }
        }

        for (String keyword : uniqueEmits) {
          String conceptId = conceptLookup.get(keyword).conceptId.toString();
          textToAnalyze = textToAnalyze.replaceAll(keyword, "__" + keyword + ":" + conceptId + "__");
        }
        nl.item(i).setTextContent(textToAnalyze);
      }

      StringWriter sw = new StringWriter();
      TransformerFactory tf = TransformerFactory.newInstance();
      Transformer transformer = tf.newTransformer();
      transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
      transformer.setOutputProperty(OutputKeys.METHOD, "xml");
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

      transformer.transform(new DOMSource(doc), new StreamResult(sw));

      result = sw.toString();
    } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException | DOMException | IllegalArgumentException | TransformerException ex) {
      throw new RuntimeException(ex.getMessage());
    }

    return result;
  }
}
