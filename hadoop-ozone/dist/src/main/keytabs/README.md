<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

This directory contains pre-generated keytabs to testing Apache Ozone in a secure environments.

The keytabs files are generated during the build of [apache/ozone-testkrb5](https://hub.docker.com/r/apache/ozone-testkrb5) image (source is managed in the [apache/ozone-docker-testkrb5](github.com/apache/ozone-docker-testkrb5) repository).

Each new container image has new keytabs, which can be downloaded / exported from the image with the included `./update-keytabs.sh` script.

THESE KEYTABS ARE FOR DEVELOPMENT/TEST ONLY, NOT FOR PRODUCTION!
