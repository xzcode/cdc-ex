package com.cdcex.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SecretBoxTest {
    @TempDir Path directory;
    @Test void authenticatedEncryptionRejectsRecordSubstitutionAndTampering()throws Exception{
        byte[] key=new byte[32];new java.security.SecureRandom().nextBytes(key);var box=new SecretBox(key);
        byte[] one=box.seal("connection:a","sensitive"),two=box.seal("connection:a","sensitive");
        assertFalse(Arrays.equals(one,two));assertEquals("sensitive",box.open("connection:a",one));
        assertThrows(Exception.class,()->box.open("connection:b",one));one[one.length-1]^=1;assertThrows(Exception.class,()->box.open("connection:a",one));
    }
    @Test void deploymentKeyAndEncryptedRecordsSurviveRestart()throws Exception{
        byte[] key=new byte[32];new java.security.SecureRandom().nextBytes(key);Path file=directory.resolve("control.db");
        try(var store=new ControlStore(file,key)){store.create("connection","source","Source","{\"password\":\"never-plaintext\"}");}
        try(var store=new ControlStore(file,key)){assertTrue(store.get("connection","source").json().contains("never-plaintext"));}
        assertFalse(new String(java.nio.file.Files.readAllBytes(file),java.nio.charset.StandardCharsets.ISO_8859_1).contains("never-plaintext"));
        key[0]^=1;assertThrows(Exception.class,()->new ControlStore(file,key));
    }
}
