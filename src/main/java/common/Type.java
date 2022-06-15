package common;

import storage.DoubleField;
import storage.Field;
import storage.IntField;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;

/**
 * @author HP
 * @date 2022/5/21
 */
public enum  Type implements Serializable {
    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new IntField(dis.readInt());
            }  catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

    }, DOUBLE_TYPE(){
        @Override
        public int getLen() {
            return 8;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new DoubleField(dis.readDouble());
            }  catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
    };

    public abstract int getLen();

    public abstract Field parse(DataInputStream dis) throws ParseException;
}
