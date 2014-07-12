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
package fi.aalto.hacid;

/**
 * Indicates that some process (HAcidClient) attempted to change a state of some
 * transaction, while that state was not anymore 'active'. The state 'active' is
 * the only one that can be overwritten. Any other overwrite means a disagreement,
 * i.e., a process considered the state to be 'active', but it was something else.
 * 
 * @author Andre Medeiros <andre.medeiros@aalto.fi>
 */
class StateDisagreementException extends Exception {

    public StateDisagreementException() {

    }
}
