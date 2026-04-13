package kr.go.kahis.batchmonitor.common.generator;

import io.hypersistence.tsid.TSID;
import io.hypersistence.tsid.TSID.Factory;
import io.hypersistence.utils.common.ReflectionUtils;
import java.lang.reflect.Member;
import java.time.ZonedDateTime;
import java.util.function.Supplier;
import kr.go.kahis.batchmonitor.common.annotation.TsuId;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.factory.spi.CustomIdGeneratorCreationContext;
import org.springframework.util.StringUtils;

public class TsuIdGenerator implements IdentifierGenerator {

  private final Factory factory;

  /**
   * Prefix 문자
   *
   * @apiNote Prefix 문자<br>
   *          ID 유형이 String 인 경우에만 적용됨
   */
  private final String prefix;

  /**
   * 구분 문자
   *
   * @apiNote 구분 문자<br>
   *          ID 유형이 String 인 경우에만 적용됨
   */
  private final String separate;

  /**
   * 생성 시점의 날짜 시간을 ID 에 포함할지 여부
   *
   * @apiNote 생성 시점의 날짜 시간을 ID 에 포함할지 여부<br>
   *          milliseconds 단위로 변환되어 ID에 포함됨<br>
   *          ID 유형이 String 인 경우에만 적용됨
   */
  private final boolean useEpoch;

  private final AttributeType attributeType;

  public TsuIdGenerator(TsuId config, Member member, CustomIdGeneratorCreationContext context) {
    attributeType = AttributeType.valueOf(ReflectionUtils.getMemberType(member));

    Class<? extends Supplier<Factory>> supplierClass = config.value();

    prefix = config.prefix();
    separate = config.separate();
    useEpoch = config.useEpoch();

    if (supplierClass.equals(TsuId.FactorySupplier.class)) {
      factory = TsuId.FactorySupplier.INSTANCE.get();
    } else {
      Supplier<Factory> supplier = ReflectionUtils.newInstance(supplierClass);

      factory = supplier.get();
    }
  }

  @Override
  public Object generate(SharedSessionContractImplementor session, Object object)
      throws HibernateException {
    return attributeType.cast(prefix, separate, useEpoch, factory.generate());
  }

  enum AttributeType {
    LONG {
      @Override
      public Object cast(String prefix, String separate, boolean useEpoch, TSID tsid) {
        return tsid.toLong();
      }
    },
    STRING {
      @Override
      public Object cast(String prefix, String separate, boolean useEpoch, TSID tsid) {
        StringBuilder idBuilder = new StringBuilder();

        if (StringUtils.hasLength(prefix)) {
          idBuilder.append(prefix).append(separate);
        }

        if (useEpoch) {
          long now = ZonedDateTime.now().toInstant().toEpochMilli();

          idBuilder.append(now).append(separate);
        }

        idBuilder.append(tsid.toString());

        return idBuilder.toString();
      }
    },
    TSID {
      @Override
      public Object cast(String prefix, String separate, boolean useEpoch, TSID tsid) {
        return tsid;
      }
    };

    public abstract Object cast(String prefix, String separate, boolean useEpoch, TSID tsid);

    static AttributeType valueOf(Class<?> clazz) {
      if (Long.class.isAssignableFrom(clazz)) {
        return LONG;
      } else if (String.class.isAssignableFrom(clazz)) {
        return STRING;
      } else if (TSID.class.isAssignableFrom(clazz)) {
        return TSID;
      } else {
        throw new HibernateException(
            String.format(
                "The @Tsid annotation on [%s] can only be placed on a Long or String entity attribute!",
                clazz
            )
        );
      }
    }
  }
}
