/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __RENDLER_HELPER_HPP__
#define __RENDLER_HELPER_HPP__

#include <string>
#include <vector>
#include <stout/stringify.hpp>
#include <stout/numify.hpp>

using std::string;
using std::vector;

namespace mesos {
  string vectorToString(vector<string>& vec) {
    string result;
    result.append(stringify<size_t>(vec.size()));
    result.push_back('\0');
    for (size_t i = 0; i < vec.size(); i++) {
      result.append(vec[i]);
      result.push_back('\0');
    }
    return result;
  }

  vector<string> stringToVector(const string& str) {
    const char *data = str.c_str();
    string lenStr = data;
    size_t len = numify<size_t>(data).get();
    data += lenStr.length() + 1;

    vector<string> result;
    for (size_t i = 0; i < len; i ++) {
      string s = data;
      data += s.length() + 1;
      result.push_back(s);
    }
    return result;
  }

};
#endif // __RENDLER_HELPER_HPP__
