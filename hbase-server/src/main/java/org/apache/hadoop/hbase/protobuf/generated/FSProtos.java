// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: FS.proto

package org.apache.hadoop.hbase.protobuf.generated;

public final class FSProtos {
  private FSProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface HBaseVersionFileContentOrBuilder
      extends com.google.protobuf.MessageOrBuilder {
    
    // required string version = 1;
    boolean hasVersion();
    String getVersion();
  }
  public static final class HBaseVersionFileContent extends
      com.google.protobuf.GeneratedMessage
      implements HBaseVersionFileContentOrBuilder {
    // Use HBaseVersionFileContent.newBuilder() to construct.
    private HBaseVersionFileContent(Builder builder) {
      super(builder);
    }
    private HBaseVersionFileContent(boolean noInit) {}
    
    private static final HBaseVersionFileContent defaultInstance;
    public static HBaseVersionFileContent getDefaultInstance() {
      return defaultInstance;
    }
    
    public HBaseVersionFileContent getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.hadoop.hbase.protobuf.generated.FSProtos.internal_static_HBaseVersionFileContent_descriptor;
    }
    
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.hadoop.hbase.protobuf.generated.FSProtos.internal_static_HBaseVersionFileContent_fieldAccessorTable;
    }
    
    private int bitField0_;
    // required string version = 1;
    public static final int VERSION_FIELD_NUMBER = 1;
    private java.lang.Object version_;
    public boolean hasVersion() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    public String getVersion() {
      java.lang.Object ref = version_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        if (com.google.protobuf.Internal.isValidUtf8(bs)) {
          version_ = s;
        }
        return s;
      }
    }
    private com.google.protobuf.ByteString getVersionBytes() {
      java.lang.Object ref = version_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8((String) ref);
        version_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    
    private void initFields() {
      version_ = "";
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;
      
      if (!hasVersion()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }
    
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getVersionBytes());
      }
      getUnknownFields().writeTo(output);
    }
    
    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;
    
      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getVersionBytes());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }
    
    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }
    
    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent)) {
        return super.equals(obj);
      }
      org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent other = (org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent) obj;
      
      boolean result = true;
      result = result && (hasVersion() == other.hasVersion());
      if (hasVersion()) {
        result = result && getVersion()
            .equals(other.getVersion());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }
    
    @java.lang.Override
    public int hashCode() {
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasVersion()) {
        hash = (37 * hash) + VERSION_FIELD_NUMBER;
        hash = (53 * hash) + getVersion().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      return hash;
    }
    
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }
    
    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContentOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.internal_static_HBaseVersionFileContent_descriptor;
      }
      
      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.internal_static_HBaseVersionFileContent_fieldAccessorTable;
      }
      
      // Construct using org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }
      
      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }
      
      public Builder clear() {
        super.clear();
        version_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      
      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }
      
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent.getDescriptor();
      }
      
      public org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent getDefaultInstanceForType() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent.getDefaultInstance();
      }
      
      public org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent build() {
        org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }
      
      private org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return result;
      }
      
      public org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent buildPartial() {
        org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent result = new org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.version_ = version_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }
      
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent) {
          return mergeFrom((org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }
      
      public Builder mergeFrom(org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent other) {
        if (other == org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent.getDefaultInstance()) return this;
        if (other.hasVersion()) {
          setVersion(other.getVersion());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }
      
      public final boolean isInitialized() {
        if (!hasVersion()) {
          
          return false;
        }
        return true;
      }
      
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder(
            this.getUnknownFields());
        while (true) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              this.setUnknownFields(unknownFields.build());
              onChanged();
              return this;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                this.setUnknownFields(unknownFields.build());
                onChanged();
                return this;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              version_ = input.readBytes();
              break;
            }
          }
        }
      }
      
      private int bitField0_;
      
      // required string version = 1;
      private java.lang.Object version_ = "";
      public boolean hasVersion() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      public String getVersion() {
        java.lang.Object ref = version_;
        if (!(ref instanceof String)) {
          String s = ((com.google.protobuf.ByteString) ref).toStringUtf8();
          version_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      public Builder setVersion(String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        version_ = value;
        onChanged();
        return this;
      }
      public Builder clearVersion() {
        bitField0_ = (bitField0_ & ~0x00000001);
        version_ = getDefaultInstance().getVersion();
        onChanged();
        return this;
      }
      void setVersion(com.google.protobuf.ByteString value) {
        bitField0_ |= 0x00000001;
        version_ = value;
        onChanged();
      }
      
      // @@protoc_insertion_point(builder_scope:HBaseVersionFileContent)
    }
    
    static {
      defaultInstance = new HBaseVersionFileContent(true);
      defaultInstance.initFields();
    }
    
    // @@protoc_insertion_point(class_scope:HBaseVersionFileContent)
  }
  
  public interface ReferenceOrBuilder
      extends com.google.protobuf.MessageOrBuilder {
    
    // required bytes splitkey = 1;
    boolean hasSplitkey();
    com.google.protobuf.ByteString getSplitkey();
    
    // required .Reference.Range range = 2;
    boolean hasRange();
    org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range getRange();
  }
  public static final class Reference extends
      com.google.protobuf.GeneratedMessage
      implements ReferenceOrBuilder {
    // Use Reference.newBuilder() to construct.
    private Reference(Builder builder) {
      super(builder);
    }
    private Reference(boolean noInit) {}
    
    private static final Reference defaultInstance;
    public static Reference getDefaultInstance() {
      return defaultInstance;
    }
    
    public Reference getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.hadoop.hbase.protobuf.generated.FSProtos.internal_static_Reference_descriptor;
    }
    
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.hadoop.hbase.protobuf.generated.FSProtos.internal_static_Reference_fieldAccessorTable;
    }
    
    public enum Range
        implements com.google.protobuf.ProtocolMessageEnum {
      TOP(0, 0),
      BOTTOM(1, 1),
      WHOLE(2, 2),
      ;
      
      public static final int TOP_VALUE = 0;
      public static final int BOTTOM_VALUE = 1;
      public static final int WHOLE_VALUE = 2;
      
      
      public final int getNumber() { return value; }
      
      public static Range valueOf(int value) {
        switch (value) {
          case 0: return TOP;
          case 1: return BOTTOM;
          case 2: return WHOLE;
          default: return null;
        }
      }
      
      public static com.google.protobuf.Internal.EnumLiteMap<Range>
          internalGetValueMap() {
        return internalValueMap;
      }
      private static com.google.protobuf.Internal.EnumLiteMap<Range>
          internalValueMap =
            new com.google.protobuf.Internal.EnumLiteMap<Range>() {
              public Range findValueByNumber(int number) {
                return Range.valueOf(number);
              }
            };
      
      public final com.google.protobuf.Descriptors.EnumValueDescriptor
          getValueDescriptor() {
        return getDescriptor().getValues().get(index);
      }
      public final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptorForType() {
        return getDescriptor();
      }
      public static final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptor() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.getDescriptor().getEnumTypes().get(0);
      }
      
      private static final Range[] VALUES = {
        TOP, BOTTOM, WHOLE, 
      };
      
      public static Range valueOf(
          com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new java.lang.IllegalArgumentException(
            "EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }
      
      private final int index;
      private final int value;
      
      private Range(int index, int value) {
        this.index = index;
        this.value = value;
      }
      
      // @@protoc_insertion_point(enum_scope:Reference.Range)
    }
    
    private int bitField0_;
    // required bytes splitkey = 1;
    public static final int SPLITKEY_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString splitkey_;
    public boolean hasSplitkey() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    public com.google.protobuf.ByteString getSplitkey() {
      return splitkey_;
    }
    
    // required .Reference.Range range = 2;
    public static final int RANGE_FIELD_NUMBER = 2;
    private org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range range_;
    public boolean hasRange() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    public org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range getRange() {
      return range_;
    }
    
    private void initFields() {
      splitkey_ = com.google.protobuf.ByteString.EMPTY;
      range_ = org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range.TOP;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;
      
      if (!hasSplitkey()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasRange()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }
    
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, splitkey_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeEnum(2, range_.getNumber());
      }
      getUnknownFields().writeTo(output);
    }
    
    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;
    
      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, splitkey_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(2, range_.getNumber());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }
    
    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }
    
    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference)) {
        return super.equals(obj);
      }
      org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference other = (org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference) obj;
      
      boolean result = true;
      result = result && (hasSplitkey() == other.hasSplitkey());
      if (hasSplitkey()) {
        result = result && getSplitkey()
            .equals(other.getSplitkey());
      }
      result = result && (hasRange() == other.hasRange());
      if (hasRange()) {
        result = result &&
            (getRange() == other.getRange());
      }
      result = result &&
          getUnknownFields().equals(other.getUnknownFields());
      return result;
    }
    
    @java.lang.Override
    public int hashCode() {
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      if (hasSplitkey()) {
        hash = (37 * hash) + SPLITKEY_FIELD_NUMBER;
        hash = (53 * hash) + getSplitkey().hashCode();
      }
      if (hasRange()) {
        hash = (37 * hash) + RANGE_FIELD_NUMBER;
        hash = (53 * hash) + hashEnum(getRange());
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      return hash;
    }
    
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }
    
    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements org.apache.hadoop.hbase.protobuf.generated.FSProtos.ReferenceOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.internal_static_Reference_descriptor;
      }
      
      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.internal_static_Reference_fieldAccessorTable;
      }
      
      // Construct using org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }
      
      private Builder(BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }
      
      public Builder clear() {
        super.clear();
        splitkey_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        range_ = org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range.TOP;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }
      
      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }
      
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.getDescriptor();
      }
      
      public org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference getDefaultInstanceForType() {
        return org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.getDefaultInstance();
      }
      
      public org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference build() {
        org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }
      
      private org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return result;
      }
      
      public org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference buildPartial() {
        org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference result = new org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.splitkey_ = splitkey_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.range_ = range_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }
      
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference) {
          return mergeFrom((org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }
      
      public Builder mergeFrom(org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference other) {
        if (other == org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.getDefaultInstance()) return this;
        if (other.hasSplitkey()) {
          setSplitkey(other.getSplitkey());
        }
        if (other.hasRange()) {
          setRange(other.getRange());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }
      
      public final boolean isInitialized() {
        if (!hasSplitkey()) {
          
          return false;
        }
        if (!hasRange()) {
          
          return false;
        }
        return true;
      }
      
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder(
            this.getUnknownFields());
        while (true) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              this.setUnknownFields(unknownFields.build());
              onChanged();
              return this;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                this.setUnknownFields(unknownFields.build());
                onChanged();
                return this;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              splitkey_ = input.readBytes();
              break;
            }
            case 16: {
              int rawValue = input.readEnum();
              org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range value = org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(2, rawValue);
              } else {
                bitField0_ |= 0x00000002;
                range_ = value;
              }
              break;
            }
          }
        }
      }
      
      private int bitField0_;
      
      // required bytes splitkey = 1;
      private com.google.protobuf.ByteString splitkey_ = com.google.protobuf.ByteString.EMPTY;
      public boolean hasSplitkey() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      public com.google.protobuf.ByteString getSplitkey() {
        return splitkey_;
      }
      public Builder setSplitkey(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        splitkey_ = value;
        onChanged();
        return this;
      }
      public Builder clearSplitkey() {
        bitField0_ = (bitField0_ & ~0x00000001);
        splitkey_ = getDefaultInstance().getSplitkey();
        onChanged();
        return this;
      }
      
      // required .Reference.Range range = 2;
      private org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range range_ = org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range.TOP;
      public boolean hasRange() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      public org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range getRange() {
        return range_;
      }
      public Builder setRange(org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000002;
        range_ = value;
        onChanged();
        return this;
      }
      public Builder clearRange() {
        bitField0_ = (bitField0_ & ~0x00000002);
        range_ = org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Range.TOP;
        onChanged();
        return this;
      }
      
      // @@protoc_insertion_point(builder_scope:Reference)
    }
    
    static {
      defaultInstance = new Reference(true);
      defaultInstance.initFields();
    }
    
    // @@protoc_insertion_point(class_scope:Reference)
  }
  
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_HBaseVersionFileContent_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_HBaseVersionFileContent_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_Reference_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_Reference_fieldAccessorTable;
  
  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\010FS.proto\"*\n\027HBaseVersionFileContent\022\017\n" +
      "\007version\030\001 \002(\t\"g\n\tReference\022\020\n\010splitkey\030" +
      "\001 \002(\014\022\037\n\005range\030\002 \002(\0162\020.Reference.Range\"\'" +
      "\n\005Range\022\007\n\003TOP\020\000\022\n\n\006BOTTOM\020\001\022\t\n\005WHOLE\020\002B" +
      ";\n*org.apache.hadoop.hbase.protobuf.gene" +
      "ratedB\010FSProtosH\001\240\001\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_HBaseVersionFileContent_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_HBaseVersionFileContent_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_HBaseVersionFileContent_descriptor,
              new java.lang.String[] { "Version", },
              org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent.class,
              org.apache.hadoop.hbase.protobuf.generated.FSProtos.HBaseVersionFileContent.Builder.class);
          internal_static_Reference_descriptor =
            getDescriptor().getMessageTypes().get(1);
          internal_static_Reference_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_Reference_descriptor,
              new java.lang.String[] { "Splitkey", "Range", },
              org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.class,
              org.apache.hadoop.hbase.protobuf.generated.FSProtos.Reference.Builder.class);
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }
  
  // @@protoc_insertion_point(outer_class_scope)
}
