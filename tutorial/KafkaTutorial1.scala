/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
import com.twitter.scalding._


class WriteFileToKafkaJob(args : Args) extends Job(args) {
  TextLine("alice.txt")
    .read
    .map('line -> 'rot13) { line : String => line map {
      case c if 'a' <= c.toLower && c.toLower <= 'm' => (c + 13).toChar
      case c if 'n' <= c.toLower && c.toLower <= 'z' => (c - 13).toChar
      case c => c } }
    .project('rot13)
    //.write(TextLine("alice_rot13.txt"))
    .write(KafkaSink("localhost:9092", "ciphertext"))
}
