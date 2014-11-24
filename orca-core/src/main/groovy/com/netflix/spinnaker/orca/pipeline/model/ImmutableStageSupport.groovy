/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.pipeline.model

import com.google.common.collect.ImmutableMap
import com.netflix.spinnaker.orca.ExecutionStatus
import java.lang.reflect.Method
import net.sf.cglib.proxy.*

class ImmutableStageSupport {

  static def <T extends Stage> T toImmutable(T stage) {
    final def immutableDelegate = new ImmutableStage(self: stage)
    def enhancer = new Enhancer()
    enhancer.superclass = stage.class
    enhancer.interfaces = [Stage]
    enhancer.callback = new MethodInterceptor() {
      @Override
      Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
        Method proxyMeth = immutableDelegate.getClass()
          .getDeclaredMethod(method.name, objects.collect { it.class } as Class[])
        proxyMeth.accessible = true
        proxyMeth.invoke(immutableDelegate, objects)
      }
    }
    (T) enhancer.create()
  }

  static class ImmutableStage<T extends Execution> implements Stage<T> {
    Stage<T> self
    ImmutableMap<String, Object> context
    boolean immutable = true

    @Override
    String getType() {
      self.type
    }

    @Override
    Execution<T> getExecution() {
      self.execution
    }

    @Override
    ExecutionStatus getStatus() {
      self.status
    }

    @Override
    void setStatus(ExecutionStatus status) {

    }

    @Override
    Stage preceding(String type) {
      self.preceding(type)?.asImmutable()
    }

    @Override
    ImmutableMap<String, Object> getContext() {
      if (!this.context) {
        this.context = ImmutableMap.copyOf(self.context)
      }
      this.context
    }

    void setContext(ImmutableMap<String, Object> context) {
      this.context = context
    }

    @Override
    Stage<T> asImmutable() {
      this
    }

    Stage<T> unwrap() {
      self
    }

    @Override
    public String toString() {
      self.toString()
    }
  }
}