package com.cdcex.storage;

import javax.crypto.Cipher;
import javax.crypto.spec.*;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;

/** Versioned AES-256-GCM records with purpose/id binding to prevent ciphertext substitution. */
public final class SecretBox {
    private final SecretKeySpec key;
    private final SecureRandom random=new SecureRandom();
    public SecretBox(byte[] key){if(key==null||key.length!=32)throw new IllegalArgumentException("Master key must contain 32 bytes");this.key=new SecretKeySpec(key.clone(),"AES");}
    public byte[] seal(String purpose,String text)throws Exception{
        byte[] nonce=new byte[12];random.nextBytes(nonce);Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE,key,new GCMParameterSpec(128,nonce));cipher.updateAAD(purpose.getBytes(StandardCharsets.UTF_8));
        byte[] encrypted=cipher.doFinal(text.getBytes(StandardCharsets.UTF_8));
        byte[] out=new byte[1+nonce.length+encrypted.length];out[0]=1;System.arraycopy(nonce,0,out,1,12);System.arraycopy(encrypted,0,out,13,encrypted.length);return out;
    }
    public String open(String purpose,byte[] bytes)throws Exception{
        if(bytes==null||bytes.length<29||bytes[0]!=1)throw new IllegalStateException("Invalid encrypted configuration");
        Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.DECRYPT_MODE,key,new GCMParameterSpec(128,Arrays.copyOfRange(bytes,1,13)));
        cipher.updateAAD(purpose.getBytes(StandardCharsets.UTF_8));return new String(cipher.doFinal(bytes,13,bytes.length-13),StandardCharsets.UTF_8);
    }
    @Override public String toString(){return "SecretBox[key=redacted]";}
}
