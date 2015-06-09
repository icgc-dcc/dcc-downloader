// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: archive.proto

package org.icgc.dcc.downloader.core;

public final class ArchiveProto {

  private ArchiveProto() {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }

  public interface DonorSelectionOrBuilder extends
      com.google.protobuf.MessageOrBuilder {

    // repeated uint32 id = 1 [packed = true];
    /**
     * <code>repeated uint32 id = 1 [packed = true];</code>
     */
    java.util.List<java.lang.Integer> getIdList();

    /**
     * <code>repeated uint32 id = 1 [packed = true];</code>
     */
    int getIdCount();

    /**
     * <code>repeated uint32 id = 1 [packed = true];</code>
     */
    int getId(int index);
  }

  /**
   * Protobuf type {@code downloader.DonorSelection}
   */
  public static final class DonorSelection extends
      com.google.protobuf.GeneratedMessage implements
      DonorSelectionOrBuilder {

    // Use DonorSelection.newBuilder() to construct.
    private DonorSelection(
        com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }

    private DonorSelection(boolean noInit) {
      this.unknownFields = com.google.protobuf.UnknownFieldSet
          .getDefaultInstance();
    }

    private static final DonorSelection defaultInstance;

    public static DonorSelection getDefaultInstance() {
      return defaultInstance;
    }

    public DonorSelection getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    private DonorSelection(com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields = com.google.protobuf.UnknownFieldSet
          .newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!parseUnknownField(input, unknownFields,
                extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
          case 8: {
            if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
              id_ = new java.util.ArrayList<java.lang.Integer>();
              mutable_bitField0_ |= 0x00000001;
            }
            id_.add(input.readUInt32());
            break;
          }
          case 10: {
            int length = input.readRawVarint32();
            int limit = input.pushLimit(length);
            if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)
                && input.getBytesUntilLimit() > 0) {
              id_ = new java.util.ArrayList<java.lang.Integer>();
              mutable_bitField0_ |= 0x00000001;
            }
            while (input.getBytesUntilLimit() > 0) {
              id_.add(input.readUInt32());
            }
            input.popLimit(limit);
            break;
          }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
          id_ = java.util.Collections.unmodifiableList(id_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.icgc.dcc.downloader.core.ArchiveProto.internal_static_downloader_DonorSelection_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
      return org.icgc.dcc.downloader.core.ArchiveProto.internal_static_downloader_DonorSelection_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection.class,
              org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection.Builder.class);
    }

    public static com.google.protobuf.Parser<DonorSelection> PARSER =
        new com.google.protobuf.AbstractParser<DonorSelection>() {

          public DonorSelection parsePartialFrom(
              com.google.protobuf.CodedInputStream input,
              com.google.protobuf.ExtensionRegistryLite extensionRegistry)
              throws com.google.protobuf.InvalidProtocolBufferException {
            return new DonorSelection(input, extensionRegistry);
          }
        };

    @java.lang.Override
    public com.google.protobuf.Parser<DonorSelection> getParserForType() {
      return PARSER;
    }

    // repeated uint32 id = 1 [packed = true];
    public static final int ID_FIELD_NUMBER = 1;
    private java.util.List<java.lang.Integer> id_;

    /**
     * <code>repeated uint32 id = 1 [packed = true];</code>
     */
    public java.util.List<java.lang.Integer> getIdList() {
      return id_;
    }

    /**
     * <code>repeated uint32 id = 1 [packed = true];</code>
     */
    public int getIdCount() {
      return id_.size();
    }

    /**
     * <code>repeated uint32 id = 1 [packed = true];</code>
     */
    public int getId(int index) {
      return id_.get(index);
    }

    private int idMemoizedSerializedSize = -1;

    private void initFields() {
      id_ = java.util.Collections.emptyList();
    }

    private byte memoizedIsInitialized = -1;

    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
        throws java.io.IOException {
      getSerializedSize();
      if (getIdList().size() > 0) {
        output.writeRawVarint32(10);
        output.writeRawVarint32(idMemoizedSerializedSize);
      }
      for (int i = 0; i < id_.size(); i++) {
        output.writeUInt32NoTag(id_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;

    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      {
        int dataSize = 0;
        for (int i = 0; i < id_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
              .computeUInt32SizeNoTag(id_.get(i));
        }
        size += dataSize;
        if (!getIdList().isEmpty()) {
          size += 1;
          size += com.google.protobuf.CodedOutputStream
              .computeInt32SizeNoTag(dataSize);
        }
        idMemoizedSerializedSize = dataSize;
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

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseFrom(
        byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseFrom(
        java.io.InputStream input) throws java.io.IOException {
      return PARSER.parseFrom(input);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseDelimitedFrom(
        java.io.InputStream input) throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }

    public static org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() {
      return Builder.create();
    }

    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder(
        org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection prototype) {
      return newBuilder().mergeFrom(prototype);
    }

    public Builder toBuilder() {
      return newBuilder(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }

    /**
     * Protobuf type {@code downloader.DonorSelection}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
        implements
        org.icgc.dcc.downloader.core.ArchiveProto.DonorSelectionOrBuilder {

      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.icgc.dcc.downloader.core.ArchiveProto.internal_static_downloader_DonorSelection_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable internalGetFieldAccessorTable() {
        return org.icgc.dcc.downloader.core.ArchiveProto.internal_static_downloader_DonorSelection_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection.class,
                org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection.Builder.class);
      }

      // Construct using
      // org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
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
        id_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.icgc.dcc.downloader.core.ArchiveProto.internal_static_downloader_DonorSelection_descriptor;
      }

      public org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection getDefaultInstanceForType() {
        return org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection
            .getDefaultInstance();
      }

      public org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection build() {
        org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection buildPartial() {
        org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection result =
            new org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection(
                this);
        int from_bitField0_ = bitField0_;
        if (((bitField0_ & 0x00000001) == 0x00000001)) {
          id_ = java.util.Collections.unmodifiableList(id_);
          bitField0_ = (bitField0_ & ~0x00000001);
        }
        result.id_ = id_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection) {
          return mergeFrom((org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection other) {
        if (other == org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection
            .getDefaultInstance()) return this;
        if (!other.id_.isEmpty()) {
          if (id_.isEmpty()) {
            id_ = other.id_;
            bitField0_ = (bitField0_ & ~0x00000001);
          } else {
            ensureIdIsMutable();
            id_.addAll(other.id_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input,
              extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.icgc.dcc.downloader.core.ArchiveProto.DonorSelection) e
              .getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private int bitField0_;

      // repeated uint32 id = 1 [packed = true];
      private java.util.List<java.lang.Integer> id_ = java.util.Collections
          .emptyList();

      private void ensureIdIsMutable() {
        if (!((bitField0_ & 0x00000001) == 0x00000001)) {
          id_ = new java.util.ArrayList<java.lang.Integer>(id_);
          bitField0_ |= 0x00000001;
        }
      }

      /**
       * <code>repeated uint32 id = 1 [packed = true];</code>
       */
      public java.util.List<java.lang.Integer> getIdList() {
        return java.util.Collections.unmodifiableList(id_);
      }

      /**
       * <code>repeated uint32 id = 1 [packed = true];</code>
       */
      public int getIdCount() {
        return id_.size();
      }

      /**
       * <code>repeated uint32 id = 1 [packed = true];</code>
       */
      public int getId(int index) {
        return id_.get(index);
      }

      /**
       * <code>repeated uint32 id = 1 [packed = true];</code>
       */
      public Builder setId(int index, int value) {
        ensureIdIsMutable();
        id_.set(index, value);
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 id = 1 [packed = true];</code>
       */
      public Builder addId(int value) {
        ensureIdIsMutable();
        id_.add(value);
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 id = 1 [packed = true];</code>
       */
      public Builder addAllId(
          java.lang.Iterable<? extends java.lang.Integer> values) {
        ensureIdIsMutable();
        super.addAll(values, id_);
        onChanged();
        return this;
      }

      /**
       * <code>repeated uint32 id = 1 [packed = true];</code>
       */
      public Builder clearId() {
        id_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:downloader.DonorSelection)
    }

    static {
      defaultInstance = new DonorSelection(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:downloader.DonorSelection)
  }

  private static com.google.protobuf.Descriptors.Descriptor internal_static_downloader_DonorSelection_descriptor;
  private static com.google.protobuf.GeneratedMessage.FieldAccessorTable internal_static_downloader_DonorSelection_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
    return descriptor;
  }

  private static com.google.protobuf.Descriptors.FileDescriptor descriptor;
  static {
    java.lang.String[] descriptorData = { "\n\rarchive.proto\022\ndownloader\" \n\016DonorSele"
        + "ction\022\016\n\002id\030\001 \003(\rB\002\020\001B)\n\031org.icgc.dcc.da"
        + "ta.archiveB\014ArchiveProto" };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {

          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            internal_static_downloader_DonorSelection_descriptor = getDescriptor()
                .getMessageTypes().get(0);
            internal_static_downloader_DonorSelection_fieldAccessorTable =
                new com.google.protobuf.GeneratedMessage.FieldAccessorTable(
                    internal_static_downloader_DonorSelection_descriptor,
                    new java.lang.String[] { "Id", });
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
        .internalBuildGeneratedFileFrom(
            descriptorData,
            new com.google.protobuf.Descriptors.FileDescriptor[] {},
            assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
