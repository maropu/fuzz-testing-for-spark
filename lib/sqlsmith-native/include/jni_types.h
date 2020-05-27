/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.io/github/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef UTILS_JNI_H
#define UTILS_JNI_H

#include <string>

// Helper classes for JNI data types
class JniPrimitiveArrayPtr {
 public:
  JniPrimitiveArrayPtr(JNIEnv *env, jarray ar): env_(env), array_(ar) {
    ptr_ = env->GetPrimitiveArrayCritical(ar, 0);
  };

  ~JniPrimitiveArrayPtr() {
    env_->ReleasePrimitiveArrayCritical(array_, ptr_, 0);
  }

  void *get() { return ptr_; }

  size_t size() { return (size_t)env_->GetArrayLength(array_); }

 private:
   JNIEnv *env_;
   jarray array_;
   void *ptr_;
};

class JniString {
 public:
  JniString(JNIEnv *env, jstring jstr): env_(env), jstring_(jstr) {
    cptr_ = env->GetStringUTFChars(jstr, 0);
    cstring_ = std::string(cptr_);
  };

  ~JniString() {
    env_->ReleaseStringUTFChars(jstring_, cptr_);
  }

  const char *c_str() { return cptr_; }

  size_t len() { return (size_t)env_->GetStringLength(jstring_); }

  std::string& str() { return cstring_; }

 private:
   JNIEnv *env_;
   jstring jstring_;
   std::string cstring_;
   const char *cptr_;
};

#endif

