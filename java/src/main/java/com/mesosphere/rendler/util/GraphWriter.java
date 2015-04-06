package com.mesosphere.rendler.util;

import java.io.*;
import java.security.*;
import java.util.*;

public class GraphWriter {

  /**
   * Write the output file with a graph format.
   * 
   * @param outputFileName
   *          the output file to write to
   * @param urlToImageFileMap
   *          the map of url's to image files
   * @param edgeList
   *          the list of sources and sinks to write
   * 
   */
  public void writeDotFile(String outputFileName, Map<String, String> urlToImageFileMap,
      Map<String, Set<String>> edgeList) {
    try {
      File outputFile = new File(outputFileName);
      FileWriter fileWriter = new FileWriter(outputFile);
      fileWriter.write("digraph G {\n");
      fileWriter.write("  node [shape=box];\n");
      Set<String> urls = urlToImageFileMap.keySet();
      for (String url : urls) {
        String hashedUrl = getHashCode(url);
        String imageFileName = urlToImageFileMap.get(url);
        fileWriter.write("  " + hashedUrl + "[label=\"\" image=\"" + imageFileName + "\"];\n");
      }
      for (String url : urls) {
        Set<String> links = edgeList.get(url);
        if (links != null) {
          for (String link : links) {
            if (urls.contains(link)) {
              String hashUrlSource = getHashCode(url);
              String hashUrlSink = getHashCode(link);
              fileWriter.write("  " + hashUrlSource + " -> " + hashUrlSink + "\n");
            }
          }
        }
      }
      fileWriter.write("}\n");
      fileWriter.flush();
      fileWriter.close();
      System.out.println("Wrote results to [" + outputFileName + "]");
    } catch (Exception e) {
      System.out.println("Exception writing the results to the output file: " + e);
    }

  }

  /**
   * Get the SHA-256 hash of a string.
   * 
   * @param inputString
   *          the input string that needs to be hashed in UTF-8 format
   * @return the hashed result
   * 
   */
  private String getHashCode(String inputString) throws NoSuchAlgorithmException {
    MessageDigest mDigest = MessageDigest.getInstance("SHA-256");
    byte[] outputHash = mDigest.digest(inputString.getBytes());
    StringBuilder sb = new StringBuilder(outputHash.length * 2);
    for (byte b : outputHash)
      sb.append(String.format("%02X", b & 0xFF));
    return sb.toString();
  }

}
