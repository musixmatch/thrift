/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "TCompactProtocol.h"

namespace apache { namespace thrift { namespace protocol {

const int8_t TCompactProtocol::TTypeToCType[16] = {
    CT_STOP, // T_STOP
    0, // unused
    CT_BOOLEAN_TRUE, // T_BOOL
    CT_BYTE, // T_BYTE
    CT_DOUBLE, // T_DOUBLE
    0, // unused
    CT_I16, // T_I16
    0, // unused
    CT_I32, // T_I32
    0, // unused
    CT_I64, // T_I64
    CT_BINARY, // T_STRING
    CT_STRUCT, // T_STRUCT
    CT_MAP, // T_MAP
    CT_SET, // T_SET
    CT_LIST, // T_LIST
  };


uint32_t TCompactProtocol::writeMessageBegin(const std::string& name,
                                             const TMessageType messageType,
                                             const int32_t seqid) {
  uint32_t wsize = 0;
  wsize += writeByte(PROTOCOL_ID);
  wsize += writeByte((VERSION_N & VERSION_MASK) | (((int32_t)messageType << TYPE_SHIFT_AMOUNT) & TYPE_MASK));
  wsize += writeVarint32(seqid);
  wsize += writeString(name);
  return wsize;
}

/**
 * Write a field header containing the field id and field type. If the
 * difference between the current field id and the last one is small (< 15),
 * then the field id will be encoded in the 4 MSB as a delta. Otherwise, the
 * field id will follow the type header as a zigzag varint.
 */
uint32_t TCompactProtocol::writeFieldBegin(const char* name,
                                           const TType fieldType,
                                           const int16_t fieldId) {
  if (fieldType == T_BOOL) {
    booleanField_.name = name;
    booleanField_.fieldType = fieldType;
    booleanField_.fieldId = fieldId;
  } else {
    return writeFieldBeginInternal(name, fieldType, fieldId, -1);
  }
  return 0;
}

/**
 * Write the STOP symbol so we know there are no more fields in this struct.
 */
uint32_t TCompactProtocol::writeFieldStop() {
  return writeByte(T_STOP);
}

/**
 * Write a struct begin. This doesn't actually put anything on the wire. We
 * use it as an opportunity to put special placeholder markers on the field
 * stack so we can get the field id deltas correct.
 */
uint32_t TCompactProtocol::writeStructBegin(const char* name) {
  lastField_.push(lastFieldId_);
  lastFieldId_ = 0;
  return 0;
}

/**
 * Write a struct end. This doesn't actually put anything on the wire. We use
 * this as an opportunity to pop the last field from the current struct off
 * of the field stack.
 */
uint32_t TCompactProtocol::writeStructEnd() {
  lastFieldId_ = lastField_.top();
  lastField_.pop();
  return 0;
}

/**
 * Write a List header.
 */
uint32_t TCompactProtocol::writeListBegin(const TType elemType,
                                          const uint32_t size) {
  return writeCollectionBegin(elemType, size);
}

/**
 * Write a set header.
 */
uint32_t TCompactProtocol::writeSetBegin(const TType elemType,
                                         const uint32_t size) {
  return writeCollectionBegin(elemType, size);
}

/**
 * Write a map header. If the map is empty, omit the key and value type
 * headers, as we don't need any additional information to skip it.
 */
uint32_t TCompactProtocol::writeMapBegin(const TType keyType,
                                         const TType valType,
                                         const uint32_t size) {
  uint32_t wsize = 0;

  if (size == 0) {
    wsize += writeByte(0);
  } else {
    wsize += writeVarint32(size);
    wsize += writeByte(getCompactType(keyType) << 4 | getCompactType(valType));
  }
  return wsize;
}

/**
 * Write a boolean value. Potentially, this could be a boolean field, in
 * which case the field header info isn't written yet. If so, decide what the
 * right type header is for the value and then write the field header.
 * Otherwise, write a single byte.
 */
uint32_t TCompactProtocol::writeBool(const bool value) {
  uint32_t wsize = 0;

  if (booleanField_.name != NULL) {
    // we haven't written the field header yet
    wsize += writeFieldBeginInternal(booleanField_.name,
                                     booleanField_.fieldType,
                                     booleanField_.fieldId,
                                     value ? CT_BOOLEAN_TRUE : CT_BOOLEAN_FALSE);
    booleanField_.name = NULL;
  } else {
    // we're not part of a field, so just write the value
    wsize += writeByte(value ? CT_BOOLEAN_TRUE : CT_BOOLEAN_FALSE);
  }
  return wsize;
}

uint32_t TCompactProtocol::writeByte(const int8_t byte) {
  trans_->write((uint8_t*)&byte, 1);
  return 1;
}

/**
 * Write an i16 as a zigzag varint.
 */
uint32_t TCompactProtocol::writeI16(const int16_t i16) {
  return writeVarint32(i32ToZigZag(i16));
}

/**
 * Write an i32 as a zigzag varint.
 */
uint32_t TCompactProtocol::writeI32(const int32_t i32) {
  return writeVarint32(i32ToZigZag(i32));
}

/**
 * Write an i64 as a zigzag varint.
 */
uint32_t TCompactProtocol::writeI64(const int64_t i64) {
  return writeVarint64(i64ToZigZag(i64));
}

/**
 * Write a double to the wire as 8 bytes.
 */
uint32_t TCompactProtocol::writeDouble(const double dub) {
  int8_t buf[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  fixedLongToBytes(doubleToLongBits(dub), buf, 0);
  trans_->write((uint8_t*)buf, 8);
  return 8;
}

/**
 * Write a string to the wire with a varint size preceeding.
 */
uint32_t TCompactProtocol::writeString(const std::string& str) {
  return writeBinary(str);
}

uint32_t TCompactProtocol::writeBinary(const std::string& str) {
  uint32_t ssize = str.size();
  uint32_t wsize = writeVarint32(ssize) + ssize;
  trans_->write((uint8_t*)str.data(), ssize);
  return wsize;
}

//
// Internal Writing methods
//

/**
 * The workhorse of writeFieldBegin. It has the option of doing a
 * 'type override' of the type header. This is used specifically in the
 * boolean field case.
 */
int32_t TCompactProtocol::writeFieldBeginInternal(const char* name,
                                                  const TType fieldType,
                                                  const int16_t fieldId,
                                                  int8_t typeOverride) {
  uint32_t wsize = 0;

  // if there's a type override, use that.
  int8_t typeToWrite = (typeOverride == -1 ? getCompactType(fieldType) : typeOverride);

  // check if we can use delta encoding for the field id
  if (fieldId > lastFieldId_ && fieldId - lastFieldId_ <= 15) {
    // write them together
    wsize += writeByte((fieldId - lastFieldId_) << 4 | typeToWrite);
  } else {
    // write them separate
    wsize += writeByte(typeToWrite);
    wsize += writeI16(fieldId);
  }

  lastFieldId_ = fieldId;
  return wsize;
}

/**
 * Abstract method for writing the start of lists and sets. List and sets on
 * the wire differ only by the type indicator.
 */
uint32_t TCompactProtocol::writeCollectionBegin(int8_t elemType, int32_t size) {
  uint32_t wsize = 0;
  if (size <= 14) {
    wsize += writeByte(size << 4 | getCompactType(elemType));
  } else {
    wsize += writeByte(0xf0 | getCompactType(elemType));
    wsize += writeVarint32(size);
  }
  return wsize;
}

/**
 * Write an i32 as a varint. Results in 1-5 bytes on the wire.
 */
uint32_t TCompactProtocol::writeVarint32(int32_t n) {
  uint8_t buf[5];
  uint32_t wsize = 0;

  while (true) {
    if ((n & ~0x7F) == 0) {
      buf[wsize++] = (int8_t)n;
      break;
    } else {
      buf[wsize++] = (int8_t)((n & 0x7F) | 0x80);
      n >>= 7;
    }
  }
  trans_->write(buf, wsize);
  return wsize;
}

/**
 * Write an i64 as a varint. Results in 1-10 bytes on the wire.
 */
uint32_t TCompactProtocol::writeVarint64(int64_t n) {
  uint8_t buf[10];
  uint32_t wsize = 0;

  while (true) {
    if ((n & ~0x7FL) == 0) {
      buf[wsize++] = (int8_t)n;
      break;
    } else {
      buf[wsize++] = (int8_t)((n & 0x7F) | 0x80);
      n >>= 7;
    }
  }
  trans_->write(buf, wsize);
  return wsize;
}

/**
 * Convert l into a zigzag long. This allows negative numbers to be
 * represented compactly as a varint.
 */
int64_t TCompactProtocol::i64ToZigZag(const int64_t l) {
  return (l << 1) ^ (l >> 63);
}

/**
 * Convert n into a zigzag int. This allows negative numbers to be
 * represented compactly as a varint.
 */
int32_t TCompactProtocol::i32ToZigZag(const int32_t n) {
  return (n << 1) ^ (n >> 31);
}

/**
 * Convert a long into little-endian bytes in buf starting at off and going
 * until off+7.
 */
void TCompactProtocol::fixedLongToBytes(int64_t n, int8_t* buf, int32_t off) {
  buf[off+0] = (int8_t)( n        & 0xff);
  buf[off+1] = (int8_t)((n >> 8 ) & 0xff);
  buf[off+2] = (int8_t)((n >> 16) & 0xff);
  buf[off+3] = (int8_t)((n >> 24) & 0xff);
  buf[off+4] = (int8_t)((n >> 32) & 0xff);
  buf[off+5] = (int8_t)((n >> 40) & 0xff);
  buf[off+6] = (int8_t)((n >> 48) & 0xff);
  buf[off+7] = (int8_t)((n >> 56) & 0xff);
}

int64_t TCompactProtocol::doubleToLongBits(const double dub) {
  union {
    double d;
    int64_t j;
  } val;
  int64_t e, f;

  val.d = dub;

  e = val.j & 0x7ff0000000000000LL;
  f = val.j & 0x000fffffffffffffLL;

  if (e == 0x7ff0000000000000LL && f != 0L)
    val.j = 0x7ff8000000000000LL;

  return val.j;
}

/**
 * Given a TType value, find the appropriate TCompactProtocol.Type value
 */
int8_t TCompactProtocol::getCompactType(int8_t ttype) {
  return TTypeToCType[ttype];
}

//
// Reading Methods
//

/**
 * Read a message header.
 */
uint32_t TCompactProtocol::readMessageBegin(std::string& name,
                                            TMessageType& messageType,
                                            int32_t& seqid) {
  uint32_t rsize = 0;
  int8_t protocolId;
  int8_t versionAndType;
  int8_t version;

  rsize += readByte(protocolId);
  if (protocolId != PROTOCOL_ID) {
    throw TProtocolException(TProtocolException::BAD_VERSION, "Bad protocol identifier");
  }

  rsize += readByte(versionAndType);
  version = (int8_t)(versionAndType & VERSION_MASK);
  if (version != VERSION_N) {
    throw TProtocolException(TProtocolException::BAD_VERSION, "Bad protocol version");
  }

  messageType = (TMessageType)((versionAndType >> TYPE_SHIFT_AMOUNT) & 0x03);
  rsize += readVarint32(seqid);
  rsize += readString(name);

  return rsize;
}

/**
 * Read a struct begin. There's nothing on the wire for this, but it is our
 * opportunity to push a new struct begin marker on the field stack.
 */
uint32_t TCompactProtocol::readStructBegin(std::string& name) {
  name = "";
  lastField_.push(lastFieldId_);
  lastFieldId_ = 0;
  return 0;
}

/**
 * Doesn't actually consume any wire data, just removes the last field for
 * this struct from the field stack.
 */
uint32_t TCompactProtocol::readStructEnd() {
  lastField_.pop();
  return 0;
}

/**
 * Read a field header off the wire.
 */
uint32_t TCompactProtocol::readFieldBegin(std::string& name,
                                          TType& fieldType,
                                          int16_t& fieldId) {
  uint32_t rsize = 0;
  int8_t byte;
  int8_t type;

  rsize += readByte(byte);
  type = (byte & 0x0f);

  // if it's a stop, then we can return immediately, as the struct is over.
  if (type == T_STOP) {
    fieldType = T_STOP;
    fieldId = 0;
    return rsize;
  }

  // mask off the 4 MSB of the type header. it could contain a field id delta.
  int16_t modifier = (int16_t)((byte & 0xf0) >> 4);
  if (modifier == 0) {
    // not a delta, look ahead for the zigzag varint field id.
    rsize += readI16(fieldId);
  } else {
    fieldId = (int16_t)(lastFieldId_ + modifier);
  }
  fieldType = getTType(type);

  // if this happens to be a boolean field, the value is encoded in the type
  if (type == CT_BOOLEAN_TRUE || type == CT_BOOLEAN_FALSE) {
    // save the boolean value in a special instance variable.
    boolValue_.hasBoolValue = true;
    boolValue_.boolValue = (type == CT_BOOLEAN_TRUE ? true : false);
  }

  // push the new field onto the field stack so we can keep the deltas going.
  lastFieldId_ = fieldId;
  return rsize;
}

/**
 * Read a map header off the wire. If the size is zero, skip reading the key
 * and value type. This means that 0-length maps will yield TMaps without the
 * "correct" types.
 */
uint32_t TCompactProtocol::readMapBegin(TType& keyType,
                                        TType& valType,
                                        uint32_t& size) {
  uint32_t rsize = 0;
  int8_t kvType = 0;
  int32_t msize = 0;

  rsize += readVarint32(msize);
  if (msize != 0)
    rsize += readByte(kvType);

  if (msize < 0) {
    throw TProtocolException(TProtocolException::NEGATIVE_SIZE);
  } else if (container_limit_ && msize > container_limit_) {
    throw TProtocolException(TProtocolException::SIZE_LIMIT);
  }

  keyType = getTType((int8_t)(kvType >> 4));
  valType = getTType((int8_t)(kvType & 0xf));
  size = (uint32_t)msize;

  return rsize;
}

/**
 * Read a list header off the wire. If the list size is 0-14, the size will
 * be packed into the element type header. If it's a longer list, the 4 MSB
 * of the element type header will be 0xF, and a varint will follow with the
 * true size.
 */
uint32_t TCompactProtocol::readListBegin(TType& elemType,
                                         uint32_t& size) {
  int8_t size_and_type;
  uint32_t rsize = 0;
  int32_t lsize;

  rsize += readByte(size_and_type);

  lsize = (size_and_type >> 4) & 0x0f;
  if (lsize == 15) {
    rsize = readVarint32(lsize);
  }

  if (lsize < 0) {
    throw TProtocolException(TProtocolException::NEGATIVE_SIZE);
  } else if (container_limit_ && lsize > container_limit_) {
    throw TProtocolException(TProtocolException::SIZE_LIMIT);
  }

  elemType = getTType((int8_t)(size_and_type & 0x0f));
  size = (uint32_t)lsize;

  return rsize;
}

/**
 * Read a set header off the wire. If the set size is 0-14, the size will
 * be packed into the element type header. If it's a longer set, the 4 MSB
 * of the element type header will be 0xF, and a varint will follow with the
 * true size.
 */
uint32_t TCompactProtocol::readSetBegin(TType& elemType,
                                        uint32_t& size) {
  return readListBegin(elemType, size);
}

/**
 * Read a boolean off the wire. If this is a boolean field, the value should
 * already have been read during readFieldBegin, so we'll just consume the
 * pre-stored value. Otherwise, read a byte.
 */
uint32_t TCompactProtocol::readBool(bool& value) {
  if (boolValue_.hasBoolValue == true) {
    value = boolValue_.boolValue;
    boolValue_.hasBoolValue = false;
    return 0;
  } else {
    int8_t val;
    readByte(val);
    value = (val == CT_BOOLEAN_TRUE);
    return 1;
  }
}

/**
 * Read a single byte off the wire. Nothing interesting here.
 */
uint32_t TCompactProtocol::readByte(int8_t& byte) {
  uint8_t b[1];
  trans_->readAll(b, 1);
  byte = *(int8_t*)b;
  return 1;
}

/**
 * Read an i16 from the wire as a zigzag varint.
 */
uint32_t TCompactProtocol::readI16(int16_t& i16) {
  int32_t value;
  uint32_t rsize = readVarint32(value);
  i16 = (int16_t)zigzagToInt(value);
  return rsize;
}

/**
 * Read an i32 from the wire as a zigzag varint.
 */
uint32_t TCompactProtocol::readI32(int32_t& i32) {
  int32_t value;
  uint32_t rsize = readVarint32(value);
  i32 = zigzagToInt(value);
  return rsize;
}

/**
 * Read an i64 from the wire as a zigzag varint.
 */
uint32_t TCompactProtocol::readI64(int64_t& i64) {
  int64_t value;
  uint32_t rsize = readVarint64(value);
  i64 = zigzagToLong(value);
  return rsize;
}

/**
 * No magic here - just read a double off the wire.
 */
uint32_t TCompactProtocol::readDouble(double& dub) {
  uint8_t longBits[8];
  trans_->readAll(longBits, 8);
  dub = longBitsToDouble(bytesToLong(longBits));
  return 8;
}

uint32_t TCompactProtocol::readString(std::string& str) {
  return readBinary(str);
}

/**
 * Read a byte[] from the wire.
 */
uint32_t TCompactProtocol::readBinary(std::string& str) {
  int32_t rsize = 0;
  int32_t size;

  rsize += readVarint32(size);
  // Catch empty string case
  if (size == 0) {
    str = "";
    return rsize;
  }

  // Catch error cases
  if (size < 0) {
    throw TProtocolException(TProtocolException::NEGATIVE_SIZE);
  }
  if (string_limit_ > 0 && size > string_limit_) {
    throw TProtocolException(TProtocolException::SIZE_LIMIT);
  }

  // Use the heap here to prevent stack overflow for v. large strings
  if (size > string_buf_size_ || string_buf_ == NULL) {
    void* new_string_buf = std::realloc(string_buf_, (uint32_t)size);
    if (new_string_buf == NULL) {
      throw TProtocolException(TProtocolException::UNKNOWN, "Out of memory in TCompactProtocol::readString");
    }
    string_buf_ = (uint8_t*)new_string_buf;
    string_buf_size_ = size;
  }
  trans_->readAll(string_buf_, size);
  str.assign((char*)string_buf_, size);

  return rsize + (uint32_t)size;
}

/**
 * Read an i32 from the wire as a varint. The MSB of each byte is set
 * if there is another byte to follow. This can read up to 5 bytes.
 */
uint32_t TCompactProtocol::readVarint32(int32_t& i32) {
  int64_t val;
  uint32_t rsize = readVarint64(val);
  i32 = (int32_t)val;
  return rsize;
}

/**
 * Read an i64 from the wire as a proper varint. The MSB of each byte is set
 * if there is another byte to follow. This can read up to 10 bytes.
 */
uint32_t TCompactProtocol::readVarint64(int64_t& i64) {
  int32_t shift = 0;
  uint32_t rsize = 0;
  int8_t b;

  i64 = 0;
  while (true) {
    rsize += readByte(b);
    i64 |= (int64_t) (b & 0x7fL) << shift;
    if ((b & 0x80) != 0x80)
      break;
    shift += 7;
  }
  return rsize;
}

/**
 * Convert from zigzag int to int.
 */
int32_t TCompactProtocol::zigzagToInt(int32_t n) {
  return (n >> 1) ^ -(n & 1);
}

/**
 * Convert from zigzag long to long.
 */
int64_t TCompactProtocol::zigzagToLong(int64_t n) {
  return (n >> 1) ^ -(n & 1);
}

/**
 * Note that it's important that the mask bytes are long literals,
 * otherwise they'll default to ints, and when you shift an int left 56 bits,
 * you just get a messed up int.
 */
int64_t TCompactProtocol::bytesToLong(uint8_t* bytes) {
  uint32_t lo = ((bytes[0])
    | (bytes[1] << 8)
    | (bytes[2] << 16)
    | (bytes[3] << 24));
  uint64_t hi = ((bytes[4])
    | (bytes[5] << 8)
    | (bytes[6] << 16)
    | (bytes[7] << 24));

  return (hi << 32) | lo;
}

double TCompactProtocol::longBitsToDouble(int64_t i64) {
  union {
    double d;
    int64_t j;
  } val;

  val.j = i64;

  return val.d;
}

TType TCompactProtocol::getTType(int8_t type) {
  switch (type) {
    case T_STOP:
      return T_STOP;
    case CT_BOOLEAN_FALSE:
    case CT_BOOLEAN_TRUE:
      return T_BOOL;
    case CT_BYTE:
      return T_BYTE;
    case CT_I16:
      return T_I16;
    case CT_I32:
      return T_I32;
    case CT_I64:
      return T_I64;
    case CT_DOUBLE:
      return T_DOUBLE;
    case CT_BINARY:
      return T_STRING;
    case CT_LIST:
      return T_LIST;
    case CT_SET:
      return T_SET;
    case CT_MAP:
      return T_MAP;
    case CT_STRUCT:
      return T_STRUCT;
    default:
      throw TException("don't know what type: " + type);
  }
  return T_STOP;
}

}}} // apache::thrift::protocol
