package com.xgen.mongot.util.bson;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.bson.codecs.BsonInt64Codec;
import org.bson.codecs.Codec;
import org.bson.codecs.pojo.PropertyCodecRegistry;
import org.bson.codecs.pojo.TypeWithTypeParameters;
import org.junit.Assert;
import org.junit.Test;

public class OptionalPropertyCodecProviderTest {

  private static class OptionalLongType implements TypeWithTypeParameters<Optional<Long>> {
    public OptionalLongType() {}

    @Override
    public Class<Optional<Long>> getType() {
      return (Class<Optional<Long>>) Optional.of(1L).getClass();
    }

    @Override
    public List<? extends TypeWithTypeParameters<?>> getTypeParameters() {
      return List.of(
          new TypeWithTypeParameters<Long>() {
            @Override
            public Class<Long> getType() {
              return Long.class;
            }

            @Override
            public List<? extends TypeWithTypeParameters<?>> getTypeParameters() {
              return null;
            }
          });
    }
  }

  private static class GenericOptionalType implements TypeWithTypeParameters<Optional> {
    public GenericOptionalType() {}

    @Override
    public Class<Optional> getType() {
      return Optional.class;
    }

    @Override
    public List<? extends TypeWithTypeParameters<?>> getTypeParameters() {
      return Collections.emptyList();
    }
  }

  private static class StringType implements TypeWithTypeParameters<String> {
    public StringType() {}

    @Override
    public Class<String> getType() {
      return String.class;
    }

    @Override
    public List<? extends TypeWithTypeParameters<?>> getTypeParameters() {
      return Collections.emptyList();
    }
  }

  private static final PropertyCodecRegistry PROPERTY_CODEC_REGISTRY =
      new PropertyCodecRegistry() {
        @Override
        public <T> Codec<T> get(TypeWithTypeParameters<T> typeWithTypeParameters) {
          Class<T> clazz = typeWithTypeParameters.getType();
          if (clazz == Long.class) {
            return (Codec<T>) new BsonInt64Codec();
          }
          return null;
        }
      };

  private static final OptionalPropertyCodecProvider codecProvider =
      new OptionalPropertyCodecProvider();

  @Test
  public void subtypesOfOptionalLong() {
    Codec codec = codecProvider.get(new OptionalLongType(), PROPERTY_CODEC_REGISTRY);
    Assert.assertNotNull(
        "Optional codec provider should provide a codec for Optional subtype", codec);
    Optional<Long> expectedInstance = Optional.of(42L);

    Assert.assertEquals(
        "codec provider should return class of same type",
        codec.getEncoderClass(),
        expectedInstance.getClass());
  }

  @Test
  public void genericSubtypesOfOptional() {
    Codec codec = codecProvider.get(new GenericOptionalType(), PROPERTY_CODEC_REGISTRY);
    Assert.assertNull(
        "Optional codec provider should not provide a codec for a generic Optional", codec);
  }

  @Test
  public void notSubtypesOfOptionalLong() {
    Codec codec = codecProvider.get(new StringType(), PROPERTY_CODEC_REGISTRY);
    Assert.assertNull(
        "Optional codec provider should not provide a "
            + "codec for types that are not assignable to Optional",
        codec);
  }
}
