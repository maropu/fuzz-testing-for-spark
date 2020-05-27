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

#include "io_github_maropu_SQLSmithNative.h"
#include "jni_types.h"

// For SQLSmith
#include "sqlite.hh"
#include "grammar.hh"
#include "prod.hh"

#include <cassert>
#include <sstream>

#define sqlsmith_unreachable(msg) \
  sqlsmith_unreachable_internal(msg, __FILE__, __LINE__)

static void sqlsmith_unreachable_internal(const std::string& errMsg, const char *file, unsigned line) {
  std::stringstream msg;
  msg << "!!UNREACHABLE!! (file=" << file << " line=" << line << ") " << errMsg;
  throw msg.str();
}

static void throw_exception(JNIEnv *env, jobject& self, const std::string& errMsg) {
  jclass c = env->FindClass("io/github/maropu/SQLSmithNative");
  assert(c != 0);
  jmethodID mth_throwex = env->GetMethodID(c, "throwException", "(Ljava/lang/String;)V");
  assert(mth_throwex != 0);
  env->CallVoidMethod(self, mth_throwex, env->NewStringUTF(errMsg.c_str()));
}

static void debugPrint(const std::string& msg) {
  fprintf(stderr, "%s", msg.c_str());
}

JNIEXPORT jlong JNICALL Java_io_github_maropu_SQLSmithNative_schemaInit
    (JNIEnv *env, jobject self, jstring conninfo, jint seed) {
  JniString c(env, conninfo);
  schema_sqlite *schema = new schema_sqlite(c.str(), true);
  smith::rng.seed(seed);
  return (jlong) schema;
}

JNIEXPORT jstring JNICALL Java_io_github_maropu_SQLSmithNative_getSQLFuzz
    (JNIEnv *env, jobject self, jlong schema_) {
  schema_sqlite *schema = (schema_sqlite *) schema_;
  scope scope;
  schema->fill_scope(scope);
  shared_ptr<prod> p = statement_factory(&scope);
  std::stringstream stmt;
  p->out(stmt);
  return env->NewStringUTF(stmt.str().c_str());
}

JNIEXPORT void JNICALL Java_io_github_maropu_SQLSmithNative_free
    (JNIEnv *env, jobject self, jlong schema_) {
  schema_sqlite *schema = (schema_sqlite *) schema_;
  delete schema;
}

