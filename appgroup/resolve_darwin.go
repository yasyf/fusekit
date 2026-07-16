//go:build darwin

package appgroup

import "github.com/ebitengine/purego/objc"

func platformResolveContainer(group string) (string, error) {
	manager := objc.ID(objc.GetClass("NSFileManager")).Send(objc.RegisterName("defaultManager"))
	// Trailing NUL: purego forwards the Go string's bytes uncopied, and
	// stringWithUTF8String: copies them into the NSString during the send.
	nsGroup := objc.ID(objc.GetClass("NSString")).Send(objc.RegisterName("stringWithUTF8String:"), group+"\x00")
	url := manager.Send(objc.RegisterName("containerURLForSecurityApplicationGroupIdentifier:"), nsGroup)
	if url == 0 {
		return "", ErrNoGroupContainer
	}
	// path/UTF8String return autoreleased/interior pointers; purego copies the
	// path into Go memory here, so no autorelease pool is needed.
	nsPath := url.Send(objc.RegisterName("path"))
	return objc.Send[string](nsPath, objc.RegisterName("UTF8String")), nil
}
